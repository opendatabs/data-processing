-- The tables of the MCP server. Built from the ingestion corpus and the open data;
-- none of them is written by another project.

-- Rollback journal, not WAL: the server reads a single file from a read-only mount.
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = NORMAL;

-- What the database was built from, so a re-index with the same inputs can stop early.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- A document of the corpus that has Markdown. The fields come from the open data (`dokumente`,
-- and `tagesordnungen` for the full transcripts); `pages` counts the page markers.
CREATE TABLE IF NOT EXISTS documents (
    dok_laufnr   INTEGER PRIMARY KEY,
    signatur_dok TEXT,
    signatur_ges TEXT,
    titel_dok    TEXT,
    dokudatum    TEXT,
    pdf_url      TEXT,
    chars        INTEGER NOT NULL DEFAULT 0,
    pages        INTEGER NOT NULL DEFAULT 1,
    indexed_at   TEXT
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_documents_ges ON documents(signatur_ges);
CREATE INDEX IF NOT EXISTS idx_documents_art ON documents(titel_dok);

-- Each document's Markdown, zlib-compressed: the server quotes from here and needs no corpus.
CREATE TABLE IF NOT EXISTS document_text (
    dok_laufnr INTEGER PRIMARY KEY,
    markdown   BLOB NOT NULL
);

-- The full-text index. Contentless, because document_text holds the text, compressed; holding
-- it a second time costs almost a gigabyte. The price: FTS5's snippet() is not available, the
-- passage is cut by snippets.passage itself — which also yields the offset from which the
-- page number and the citation link are derived.
-- `kontext` holds what the extract job wrote about the document: summary, core demand,
-- measures, questions, arguments. It is searched but never quoted — it names the matter in
-- other words than the document does, which is what lexical search needs, and it was
-- written by a model, which is why no passage and no citation may come out of it.
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    titel,
    urheber,
    partei,
    body,
    kontext,
    content = '',
    contentless_delete = 1,
    tokenize = 'unicode61 remove_diacritics 2',
    prefix = '2 3'
);

-- The index's vocabulary, each term also written backwards. A search matches word starts only;
-- «ends with» on the reversed term is how a query for «Befragung» finds «Jugendbefragung»
-- (see compounds.py). Rewritten after every index build.
CREATE TABLE IF NOT EXISTS index_terms (
    term     TEXT PRIMARY KEY,
    reversed TEXT NOT NULL,
    docs     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_index_terms_reversed ON index_terms(reversed);

-- What the extract job wrote about each chunk of a document, with the pages that chunk
-- covers. The full-text index is per document and cannot hold this; kept beside it, it is
-- what lets a hit found only through the context still name a page — the passage shown is
-- then cut from the document at that page, never from this text.
CREATE TABLE IF NOT EXISTS document_context (
    dok_laufnr INTEGER NOT NULL,
    teil       INTEGER NOT NULL,
    seiten     TEXT NOT NULL DEFAULT '',
    text       TEXT NOT NULL,
    PRIMARY KEY (dok_laufnr, teil)
);

-- The graph. `nodes` are the things the council talks about, `triples` are what the data
-- claims about them. `source` says what an edge rests on: 'ogd' for what the open data
-- records explicitly, 'llm' for what the extract job read from a document — which then carries
-- the verbatim quote. A new run deletes and rebuilds only its own source, so one never
-- overwrites the other.
CREATE TABLE IF NOT EXISTS nodes (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    name       TEXT NOT NULL,
    url        TEXT,
    attrs_json TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type);

CREATE TABLE IF NOT EXISTS triples (
    subject_id TEXT NOT NULL,
    predicate  TEXT NOT NULL,
    object_id  TEXT NOT NULL,
    source     TEXT NOT NULL,
    -- Part of the key, so it must not be NULL: SQLite treats NULLs as distinct and would
    -- allow the same edge any number of times. Empty means: no role.
    qualifier  TEXT NOT NULL DEFAULT '',
    dok_laufnr INTEGER NOT NULL DEFAULT 0,
    -- Part of the key as well: someone on the same committee for two terms has one edge
    -- per term. Empty means: no start known.
    valid_from TEXT NOT NULL DEFAULT '',
    valid_to   TEXT,
    quote      TEXT,
    weight     REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (subject_id, predicate, object_id, source, qualifier, dok_laufnr, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_triples_subject ON triples(subject_id, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_object ON triples(object_id, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_source ON triples(source);

-- `degree` is in the index on purpose: by name similarity alone an association mentioned
-- once ranks next to the Universität Basel, and counting the edges per entry on every
-- query would mean scanning all triples.
CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
    name,
    type,
    node_id UNINDEXED,
    degree UNINDEXED,
    tokenize = 'unicode61 remove_diacritics 2',
    prefix = '2 3'
);

-- Every business item's topic as the topic job assigned it: the council's own label
-- (`source = 'ogd'`) or the classifier's (`source = 'classifier'`, with its confidence).
CREATE TABLE IF NOT EXISTS topic_assignments (
    signatur_ges         TEXT PRIMARY KEY,
    topic                TEXT NOT NULL,
    confidence           REAL,
    topic_secondary      TEXT,
    confidence_secondary REAL,
    source               TEXT NOT NULL,
    model                TEXT NOT NULL DEFAULT ''
);
