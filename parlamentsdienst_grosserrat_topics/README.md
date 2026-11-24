# Parlamentsdienst Grosser Rat: topics

Will give every business item (Geschäft) of the Grosser Rat a topic of the parliamentary services.
For now the job only checks its setup: the classifier, the embedding server, the Markdown of the
documents and the output folder.

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## Files

| In the container | On the share (`/mnt/OGD-DataExch/StatA/Parlament/`) | |
|---|---|---|
| `data_orig/markdown/` | `markdown/` (read-only) | the documents as Markdown, written by [parlamentsdienst_grosserrat_datasette](../parlamentsdienst_grosserrat_datasette) |
| `data/model/` | `topics/model/` | the classifier: `modell.joblib` and `karte.json` |
| `data/` | `topics/` | the output folder |

### Setup

1. Create `topics/model/` on the share.
2. Copy `modell.joblib` and `karte.json` of the current model (`20260922T123415`) into it. They
   come from the training code (`topic_classify/data/modelle/20260922T123415/` in DCC-BS's
   grossrat-bs-mcp).
3. Set the Airflow variables below.

## Environment

| Variable | |
|---|---|
| `QWEN_EMBED_URL` | the embedding server, including `/v1` |
| `QWEN_EMBED_MODEL` | the model name on that server |
| `QWEN_EMBED_API_KEY` | optional |

The server has to run the embedding weights in `karte.json` (`embed_model`).
