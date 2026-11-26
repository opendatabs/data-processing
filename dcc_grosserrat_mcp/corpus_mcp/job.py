"""The re-index: open data, index, graph, topics, signatures and passage vectors, from the mounted files."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path

import httpx

from corpus_mcp import (
    ann,
    compounds,
    context,
    failures,
    graph,
    index,
    model_edges,
    ods,
    release,
    signatures,
    topic_edges,
    vectors,
)
from corpus_mcp.db import connect, open_db, version
from corpus_mcp.settings import Settings, get_settings

logger = logging.getLogger(__name__)


def reindex(settings: Settings | None = None) -> dict[str, object]:
    """Build the MCP database afresh from the open data, the corpus, Gemma's summaries and the topics.

    It is built beside the served file and swapped in when complete: a reader never sees it half
    built, and nothing from an earlier run survives that the current sources no longer yield.
    When the inputs are the same as those of the served database, it stops after reading the open
    data and leaves the served file as it is.
    """
    settings = settings or get_settings()
    _check_files(settings)
    served = settings.db_path
    building = served.with_name(served.name + ".new")
    building.unlink(missing_ok=True)
    report: dict[str, object] = {}
    with open_db(building) as conn:
        with _step("mirroring the open data"), httpx.Client(follow_redirects=True) as client:
            report["mirrored"] = ods.mirror_all(conn, client)
        with _step("comparing the inputs with the served database"):
            inputs = _inputs(settings, conn)
            rebuilt = inputs != _built_from(served)
        if rebuilt:
            _build(conn, settings, report)
            with conn:
                conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", inputs.items())
        else:
            logger.info("the inputs are unchanged since the served database was built; nothing to rebuild")
            report["unchanged"] = True
    if rebuilt:
        building.replace(served)
        logger.info("%s swapped in", served)
        release.write(settings, report)
    else:
        building.unlink()
    if settings.embed:
        with _step("embedding new and changed documents"):
            report["embedded"] = _embed(settings, report)
    report["failed"] = failures.write(settings)
    logger.info("reindex finished: %s", json.dumps(report, ensure_ascii=False))
    return report


def _check_files(settings: Settings) -> None:
    """Log what the mounted folders hold; stop if there is no Markdown, rather than release an empty database."""
    markdown = settings.corpus_dir / "markdown"
    for path, missing in (
        (markdown, "nothing to index"),
        (settings.corpus_dir / "docling", "no passages, so no vectors for new documents"),
        (settings.topics, "the database gets no topics"),
        (settings.extract_db, "no summaries from Gemma yet; dcc_grosserrat_summaries writes it"),
        (settings.db_path, "built from scratch"),
        (settings.vectors_path, "every passage is embedded on the CPU, about two days; copy the seed first"),
        (settings.index_path, "built from the vectors"),
    ):
        if path.is_dir():
            logger.info("%s: %d files", path, sum(1 for entry in path.rglob("*") if entry.is_file()))
        elif path.exists():
            logger.info("%s: %.0f MB", path, path.stat().st_size / 1e6)
        else:
            logger.warning("%s not found: %s", path, missing)
    if not any(markdown.rglob("*.md")):
        raise RuntimeError(f"no Markdown in {markdown}: is Parlament/markdown mounted?")


def _build(conn: sqlite3.Connection, settings: Settings, report: dict[str, object]) -> None:
    """Every step after the open data, each logged with its duration."""
    with _step("reading the extract database"):
        chunks = context.read(settings.extract_db)
        report["context"] = context.store(conn, chunks)
    with _step("indexing the documents"):
        report["indexed"] = index.build(conn, settings.corpus_dir, context=context.per_document(chunks))
    with _step("writing the vocabulary for compounds"):
        report["terms"] = compounds.build(conn)
    with _step("building the graph from the open data"):
        report["graph"] = graph.build(conn)
    with _step("adding the relations from the extract database"):
        report["model_edges"] = model_edges.load(conn, settings.extract_db)
    with _step("adding the topics"):
        report["topics"] = topic_edges.load(conn, settings.topics)
    with _step("reading the co-signers"):
        report["signatures"] = signatures.load(conn, settings.corpus_dir)
    with _step("indexing the names in the graph"):
        report["entities"] = graph.index_nodes(conn)


def _inputs(settings: Settings, conn: sqlite3.Connection) -> dict[str, str]:
    """What a build depends on: the open data, the corpus, the two enrichment databases and this code."""
    return {
        "open_data": ods.digest(conn),
        "corpus": _directory(settings.corpus_dir / "markdown"),
        "extract": _file(settings.extract_db),
        "topics": _file(settings.topics),
        "code": _code(),
    }


def _built_from(served: Path) -> dict[str, str] | None:
    """The inputs the served database was built from, if it records them."""
    if not served.exists():
        return None
    with closing(connect(served, readonly=True)) as conn:
        try:
            return {key: value for key, value in conn.execute("SELECT key, value FROM meta")}
        except sqlite3.OperationalError:
            return None


def _directory(path: Path) -> str:
    """How many files a directory tree holds and when the newest changed."""
    stats = [entry.stat() for entry in path.rglob("*") if entry.is_file()]
    return f"{len(stats)} files, newest {max((s.st_mtime_ns for s in stats), default=0)}"


def _file(path: Path) -> str:
    """A file's size and modification time, or that it is missing."""
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_size} bytes, modified {stat.st_mtime_ns}"


def _code() -> str:
    """A hash over this package's source and schema."""
    hashed = hashlib.sha256()
    root = Path(__file__).parent
    for source in sorted([*root.rglob("*.py"), *root.rglob("*.sql")]):
        hashed.update(source.relative_to(root).as_posix().encode())
        hashed.update(source.read_bytes())
    return hashed.hexdigest()


@contextmanager
def _step(label: str) -> Generator[None]:
    """Log when a step of the re-index starts and how long it took."""
    logger.info("%s …", label)
    start = time.perf_counter()
    yield
    logger.info("%s: done in %.0f s", label, time.perf_counter() - start)


def _embed(settings: Settings, report: dict[str, object]) -> dict[str, int]:
    """Embed the passages of documents whose vectors are missing or stale, and release new vectors."""
    embedder = settings.embedder()
    before = version(settings.vectors_path, settings.index_path)
    with open_db(settings.db_path, readonly=True) as index_db:
        store = vectors.connect(settings.vectors_path)
        try:
            written = vectors.build(store, index_db, settings.corpus_dir, embedder, limit=settings.embed_limit or None)
            embedded = vectors.stats(store)
            # A run that stopped before building the index leaves it short of the store.
            if written["documents"] or written["skipped"] or ann.count(settings.index_path) != embedded["passages"]:
                with _step("building the nearest-neighbour index"):
                    ann.build(store, settings.index_path)
        finally:
            store.close()
    result = {**written, **embedded}
    if version(settings.vectors_path, settings.index_path) != before:
        release.write(settings, {**report, "embedded": result})
    return result
