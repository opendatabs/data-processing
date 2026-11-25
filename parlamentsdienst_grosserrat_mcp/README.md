# Parlamentsdienst Grosser Rat: MCP database

Builds the database the MCP server of the Grosser Rat reads: a full-text index with the text of each
document, the graph of business items, people and committees, the topics, and search vectors for
the passages.

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## What a run does

It mirrors the open data from [data.bs.ch](https://data.bs.ch), builds `corpus-mcp.db` beside the
served one and swaps it in when complete, then embeds the passages of new documents on the CPU.
When nothing it reads has changed, it leaves the database as it is. It logs at the start which
files it finds, and stops if there is no Markdown rather than release an empty database.

## Files

| In the container | On the share (`/mnt/OGD-DataExch/StatA/Parlament/`) | |
|---|---|---|
| `data_orig/markdown/`, `data_orig/docling/` | `markdown/`, `docling/` (read-only) | the documents, written by [parlamentsdienst_grosserrat_datasette](../parlamentsdienst_grosserrat_datasette) |
| `data_orig/topics/topics.csv` | `topics/topics.csv` (read-only) | written by [parlamentsdienst_grosserrat_topics](../parlamentsdienst_grosserrat_topics) |
| `data_orig/summaries/extract.db` | `summaries/extract.db` (read-only) | written by [parlamentsdienst_grosserrat_summaries](../parlamentsdienst_grosserrat_summaries) |
| `data/corpus-mcp.db` | `mcp/` | what the MCP server reads |
| `data/vectors.db`, `data/vectors.faiss` | `mcp/` | the passage vectors and their index |
| `data/release.json` | `mcp/` | rewritten when the database or the vectors changed |
| `data/failed.csv` | `mcp/` | the documents a step could not handle, with the reason |

The first run starts from a copy of `corpus-mcp.db`, `vectors.db` and `vectors.faiss` in `mcp/`:
embedding all passages on the CPU takes about two days. After that a run embeds only what is new.

## Environment

| Variable | |
|---|---|
| `MCP_EMBED_THREADS` | CPU threads for embedding (default: every core) |

The embedding model ([granite-embedding-311m-multilingual-r2](https://huggingface.co/ibm-granite/granite-embedding-311m-multilingual-r2))
is part of the image.
