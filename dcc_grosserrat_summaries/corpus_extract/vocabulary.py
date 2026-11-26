"""The vocabulary the council uses to talk about its business."""

from __future__ import annotations

from enum import StrEnum

from slugify import slugify


class Predicate(StrEnum):
    """Every relation the model may claim."""

    CONCERNS_ORGANISATION = "CONCERNS_ORGANISATION"
    CONCERNS_PLACE = "CONCERNS_PLACE"
    CONCERNS_TOPIC = "CONCERNS_TOPIC"
    CONCERNS_PERSON = "CONCERNS_PERSON"
    AFFECTS_GROUP = "AFFECTS_GROUP"
    CITES_LAW = "CITES_LAW"
    RETRIES = "RETRIES"


class EntityType(StrEnum):
    """What a relation may point at."""

    AUTHORITY = "behoerde"
    DEPARTMENT = "departement"
    COMMITTEE = "kommission"
    COURT = "gericht"
    PARTY = "partei"
    COMPANY = "unternehmen"
    CLUB = "verein"
    ASSOCIATION = "verband"
    FOUNDATION = "stiftung"
    COOPERATIVE = "genossenschaft"
    UNIVERSITY = "hochschule"
    SCHOOL = "schule"
    HOSPITAL = "spital"
    PERSON = "person"
    PLACE = "ort"
    TOPIC = "thema"
    LAW = "erlass"
    GROUP = "gruppe"
    BUSINESS_ITEM = "geschaeft"


ORGANISATIONS = frozenset(
    {
        EntityType.AUTHORITY,
        EntityType.DEPARTMENT,
        EntityType.COMMITTEE,
        EntityType.COURT,
        EntityType.PARTY,
        EntityType.COMPANY,
        EntityType.CLUB,
        EntityType.ASSOCIATION,
        EntityType.FOUNDATION,
        EntityType.COOPERATIVE,
        EntityType.UNIVERSITY,
        EntityType.SCHOOL,
        EntityType.HOSPITAL,
    }
)

NAMESPACE: dict[EntityType, str] = {
    **dict.fromkeys(ORGANISATIONS, "org"),
    EntityType.PERSON: "person_name",
    EntityType.PLACE: "ort",
    EntityType.TOPIC: "thema",
    EntityType.LAW: "law",
    EntityType.GROUP: "gruppe",
    EntityType.BUSINESS_ITEM: "ges",
}

ALLOWED: dict[Predicate, frozenset[EntityType]] = {
    Predicate.CONCERNS_ORGANISATION: ORGANISATIONS,
    Predicate.CONCERNS_PLACE: frozenset({EntityType.PLACE}),
    Predicate.CONCERNS_TOPIC: frozenset({EntityType.TOPIC}),
    Predicate.CONCERNS_PERSON: frozenset({EntityType.PERSON}),
    Predicate.AFFECTS_GROUP: frozenset({EntityType.GROUP}),
    Predicate.CITES_LAW: frozenset({EntityType.LAW}),
    Predicate.RETRIES: frozenset({EntityType.BUSINESS_ITEM}),
}

# German umlauts, which slugify would drop: «Universität» → «universitaet».
UMLAUTS = [["ä", "ae"], ["ö", "oe"], ["ü", "ue"], ["Ä", "ae"], ["Ö", "oe"], ["Ü", "ue"], ["ß", "ss"]]
MAX_SLUG = 60


def node_id(entity_type: EntityType, name: str) -> str:
    """Build the node id for a name the model gave."""
    slug = slugify(name, replacements=UMLAUTS, max_length=MAX_SLUG, word_boundary=True)
    return f"{NAMESPACE[entity_type]}:{slug or 'unbenannt'}"


def geschaeft_id(signatur: str) -> str:
    """The node id of a business item, from its signature."""
    parts = signatur.strip().split(".")
    return "ges:" + ".".join(parts[:2])


def allowed(predicate: str, object_type: str) -> bool:
    """Whether a stored relation's predicate and object type are in the vocabulary."""
    try:
        return EntityType(object_type) in ALLOWED[Predicate(predicate)]
    except ValueError:
        return False
