"""Co-signers of a motion from the signature line at the end of its text, as `CO_SIGNED_BY` edges.

A line counts if at least two of its names match members and names make up two thirds of it.
Names are matched exactly, by first name and a surname part, with up to two typos, or by
initial and surname among the members sitting on that date; a name matching none or several
is dropped and counted. The author is `AUTHORED_BY` already and is left out.
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from rapidfuzz.distance import Levenshtein

from corpus_mcp import corpus, progress

SOURCE = "signatures"
PREDICATE = "CO_SIGNED_BY"

# Business item types a member submits and others co-sign.
MOTIONS = (
    "Anzug",
    "Motion",
    "Interpellation",
    "Schriftliche Anfrage",
    "Kleine Anfrage",
    "Budgetpostulat",
    "Vorgezogenes Budgetpostulat",
    "Planungsanzug",
    "Planungsauftrag",
    "Resolution",
)

TAIL_LINES = 8
TITLES = {"dr", "prof", "pd", "lic", "iur", "phil", "med", "rer", "pol", "oec"}
QUOTE_CHARS = 1000

# The motion's own text: its '.01' document, or for older items the first one titled "Text …".
TEXTS = f"""
    SELECT g.signatur_ges, g.urheber_nr, g.beginn_ges, d.dok_laufnr, d.dokudatum
    FROM geschaefte g
    JOIN dokumente d ON d.dok_laufnr = (
        SELECT d2.dok_laufnr FROM dokumente d2
        WHERE d2.signatur_ges = g.signatur_ges
          AND (d2.signatur_dok = d2.signatur_ges || '.01' OR d2.titel_dok LIKE 'Text%')
        ORDER BY d2.signatur_dok = d2.signatur_ges || '.01' DESC, d2.dokudatum, CAST(d2.dok_laufnr AS INTEGER)
        LIMIT 1)
    WHERE g.ga_rr_gr IN ({",".join("?" * len(MOTIONS))})
"""

EDGE = """
    INSERT OR REPLACE INTO triples
        (subject_id, predicate, object_id, source, qualifier, dok_laufnr,
         valid_from, valid_to, quote, weight, created_at)
    SELECT ?, ?, ?, ?, ?, ?, COALESCE(?, ''), NULL, ?, NULL, ?
    WHERE ? IN (SELECT id FROM nodes) AND ? IN (SELECT id FROM nodes)
"""


def squash(text: str) -> str:
    """Letters only, lower case: 'Gysin-Roth' and 'GysinRoth' become the same key."""
    return "".join(ch for ch in unicodedata.normalize("NFC", text).lower() if ch.isalpha())


@dataclass(frozen=True, slots=True)
class Member:
    """A member of the council, as the open data lists them."""

    nr: str
    first: tuple[str, ...]
    last: frozenset[str]
    key: str
    periods: tuple[tuple[str, str], ...]

    def sat_on(self, date: str) -> bool:
        """Whether they were a member on this date (unknown dates count as yes)."""
        if not date:
            return True
        return any(
            (not begin or begin[:10] <= date[:10]) and (not end or date[:10] <= end[:10]) for begin, end in self.periods
        )


# The shortest surname part that still counts when a written name only begins with it.
SURNAME_PREFIX = 4


@dataclass(slots=True)
class Members:
    """Everyone who sat in the council, and the three ways a written name finds one of them."""

    people: list[Member]
    by_key: dict[str, list[Member]] = field(default_factory=dict)
    # Lookups, so a written name is compared with the few members it can be, not with all of them.
    by_surname: dict[str, list[Member]] = field(default_factory=dict)
    by_initial: dict[str, list[Member]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for member in self.people:
            self.by_key.setdefault(member.key, []).append(member)
            self.by_initial.setdefault(member.key[:1], []).append(member)
            for part in member.last:
                self.by_surname.setdefault(part, []).append(member)

    def with_surname(self, written: set[str]) -> list[Member]:
        """The members whose surname a written part is or begins with; a double name may have lost its hyphen."""
        found: dict[str, Member] = {}
        for word in written:
            parts = [word, *(word[:end] for end in range(SURNAME_PREFIX, len(word)))]
            for part in parts:
                for member in self.by_surname.get(part, ()):
                    found.setdefault(member.nr, member)
        return list(found.values())

    @classmethod
    def load(cls, conn: sqlite3.Connection) -> Members:
        """The members from the mirrored `personen` table: one row per term, merged per person."""
        terms: dict[str, tuple[str, list[tuple[str, str]]]] = {}
        for nr, name, begin, end in conn.execute(
            "SELECT uni_nr, name_vorname, gr_beginn, gr_ende FROM personen"
            " WHERE uni_nr != '' AND name_vorname LIKE '%, %'"
        ):
            terms.setdefault(nr, (name, []))[1].append((begin or "", end or ""))
        people = []
        for nr, (name, periods) in terms.items():
            last, first = (part.strip() for part in name.split(",", 1))
            people.append(
                Member(
                    nr=nr,
                    first=tuple(squash(t) for t in first.split()),
                    last=frozenset(squash(t) for t in re.split(r"[-\s]+", last) if t),
                    key=squash(f"{first} {last}"),
                    periods=tuple(periods),
                )
            )
        return cls(people)

    def match(self, written: str, date: str) -> tuple[Member | None, str]:
        """The member a written name stands for, and how it was found; None if not exactly one."""
        tokens = [t for t in written.split() if squash(t) not in TITLES]
        if len(tokens) < 2:
            return None, ""
        written = " ".join(tokens)
        if tokens[0].endswith(".") and len(squash(tokens[0])) <= 3:
            return self._initials(tokens, date)
        found = self._pick(self.by_key.get(squash(written), []), date)
        if found:
            return found, "exakt"
        first, rest = squash(tokens[0]), {squash(t) for t in re.split(r"[-\s]+", " ".join(tokens[1:])) if t}
        candidates = self.with_surname(rest)
        found = self._pick([m for m in candidates if first in m.first], date)
        if found:
            return found, "Namensteil"
        found = self._pick(
            [m for m in candidates if any(Levenshtein.distance(first, f) <= 1 for f in m.first)],
            date,
        )
        if found:
            return found, "Tippfehler"
        key = squash(written)
        close = [m for m in self.by_initial.get(key[:1], ()) if abs(len(m.key) - len(key)) <= 2]
        found = self._pick([m for m in close if Levenshtein.distance(m.key, key) <= 2], date)
        if found:
            return found, "Tippfehler"
        return None, ""

    def _initials(self, tokens: list[str], date: str) -> tuple[Member | None, str]:
        """A name written as initials and surname: the one member with both who sat on that date."""
        if not date:
            return None, ""
        initials = [t for t in tokens if t.endswith(".") and len(squash(t)) <= 3]
        rest = {squash(t) for t in re.split(r"[-\s]+", " ".join(t for t in tokens if t not in initials)) if squash(t)}
        letter = squash(initials[0])[:1]
        if not rest or not letter:
            return None, ""
        found = [m for m in self.with_surname(rest) if m.sat_on(date) and m.first and m.first[0][:1] == letter]
        return (found[0], "Initiale") if len(found) == 1 else (None, "")

    @staticmethod
    def _pick(candidates: list[Member], date: str) -> Member | None:
        """One candidate, or the only one who sat in the council on that date."""
        if len(candidates) == 1:
            return candidates[0]
        sitting = [m for m in candidates if m.sat_on(date)]
        return sitting[0] if len(sitting) == 1 else None


@dataclass(frozen=True, slots=True)
class Signature:
    """A recognised signature line and the members it names."""

    line: str
    signers: tuple[tuple[Member, str], ...]
    unmatched: tuple[str, ...]


def _parts(line: str) -> list[str]:
    """The names a line lists: separated by commas, semicolons or 'und'."""
    line = re.sub(r"<!--.*?-->", " ", line)
    line = re.sub(r"^[#>*\-|\s]+", "", line)
    parts = re.split(r",|;|\bund\b|\|", line)
    return [" ".join(p.split()).strip(" .:") for p in parts if p.strip(" .:")]


def read_line(text: str, members: Members, date: str) -> Signature | None:
    """The signature line among the last lines of a motion's text, or None."""
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()][-TAIL_LINES:]
    best: Signature | None = None
    for line in lines:
        signers: list[tuple[Member, str]] = []
        unmatched: list[str] = []
        for part in _parts(line):
            member, how = members.match(part, date)
            if member is None and len(part.split()) > 2:
                # A sentence run into the first name: '… einzuführen. Lukas Imhof'.
                for n in (2, 3):
                    member, how = members.match(" ".join(part.split()[-n:]), date)
                    if member:
                        how = "am Satzende"
                        break
            if member:
                signers.append((member, how))
            else:
                unmatched.append(part)
        if (
            len(signers) >= 2
            and len(signers) >= len(unmatched) * 2
            and (best is None or len(signers) > len(best.signers))
        ):
            best = Signature(line=line, signers=tuple(signers), unmatched=tuple(unmatched))
    return best


def load(conn: sqlite3.Connection, corpus_dir: Path) -> dict[str, object]:
    """Write a `signatures` edge for every signer read from the motions' texts."""
    members = Members.load(conn)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    stats: Counter[str] = Counter()
    how_found: Counter[str] = Counter()
    unmatched: Counter[str] = Counter()
    rows = conn.execute(TEXTS, MOTIONS).fetchall()
    with conn:
        for signatur, author, begin, dok_laufnr, dokudatum in progress.track(rows, label="co-signers", total=len(rows)):
            stats["texts"] += 1
            text = corpus.read(corpus_dir, int(dok_laufnr), dokudatum or "")
            if text is None:
                stats["text missing"] += 1
                continue
            signature = read_line(text, members, begin or dokudatum or "")
            if signature is None:
                continue
            stats["signature lines"] += 1
            unmatched.update(signature.unmatched)
            subject = f"ges:{signatur}"
            for member, how in signature.signers:
                how_found[how] += 1
                if member.nr == author:
                    continue
                obj = f"person:{member.nr}"
                stats["edges"] += conn.execute(
                    EDGE,
                    (
                        subject,
                        PREDICATE,
                        obj,
                        SOURCE,
                        how,
                        int(dok_laufnr),
                        begin or None,
                        signature.line[:QUOTE_CHARS],
                        now,
                        subject,
                        obj,
                    ),
                ).rowcount
    names = sum(how_found.values())
    return {
        **stats,
        "names": names,
        "matched": dict(how_found),
        "unmatched": sum(unmatched.values()),
        "unmatched_examples": [name for name, _ in unmatched.most_common(10)],
    }
