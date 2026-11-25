# Parlamentsdienst Grosser Rat: summaries

Lets Gemma read the documents of the Grosser Rat and records, per document: a summary with the core
demand, measures, questions and arguments, the words someone would search it by, and relations to
organisations, places, people, groups, laws and earlier business items. A relation is kept only if
its quote appears verbatim in the document. [parlamentsdienst_grosserrat_mcp](../parlamentsdienst_grosserrat_mcp)
takes all of it into the database of the MCP server.

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## What a run does

It reads up to 500 documents that have no summary yet or a chunk whose reading failed, motion texts
first; of a document read before it asks only the failed chunks again. Long documents are cut along
their Docling structure into chunks. Then it writes the search terms for every chunk that has a
summary and none yet. A document read by an earlier model counts as read.

## Files

| In the container | On the share (`/mnt/OGD-DataExch/StatA/Parlament/`) | |
|---|---|---|
| `data_orig/markdown/`, `data_orig/docling/` | `markdown/`, `docling/` (read-only) | the documents, written by [parlamentsdienst_grosserrat_datasette](../parlamentsdienst_grosserrat_datasette) |
| `data_orig/mcp/corpus-mcp.db` | `mcp/corpus-mcp.db` (read-only) | the list of documents and their business items |
| `data/extract.db` | `summaries/extract.db` | the output: `extractions`, `expansions` and `relations` |

The first run starts from a copy of `extract.db` in `summaries/`; without it every document is read
from scratch.

## Environment

| Variable | |
|---|---|
| `GEMMA_URL` | Gemma behind the KIS gateway, including `/v1` |
| `GEMMA_MODEL` | the model id as `/v1/models` lists it; the job checks it before it reads |
| `GEMMA_API_KEY` | the gateway's key |
