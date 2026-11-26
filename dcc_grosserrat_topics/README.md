# Parlamentsdienst Grosser Rat: topics

Gives every business item (Geschäft) of the Grosser Rat the two most likely of the 46 topics of the
parliamentary services. Items the parliamentary services labelled themselves
([100086](https://data.bs.ch/explore/dataset/100086)) keep that label. Every other item gets the
topics a classifier reads from one of its documents ([100313](https://data.bs.ch/explore/dataset/100313)).

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## Files

| In the container | On the share (`/mnt/OGD-DataExch/StatA/Parlament/`) | |
|---|---|---|
| `data_orig/markdown/` | `markdown/` (read-only) | the documents as Markdown, written by [dcc_grosserrat_docling](../dcc_grosserrat_docling) |
| `data/model/` | `topics/model/` | the classifier: `modell.joblib`, `karte.json` and its training data `datensatz.npz` |
| `data/topics.csv` | `topics/topics.csv` | the output, written by the job |

To change the model, replace the files in `topics/model/`. The next run classifies all items again.

## Environment

| Variable | |
|---|---|
| `QWEN_EMBED_URL` | the embedding server, including `/v1` |
| `QWEN_EMBED_MODEL` | the model name on that server |
| `QWEN_EMBED_API_KEY` | optional |

The server has to run the embedding weights in `karte.json` (`embed_model`). The job checks this
before it classifies anything.

## Output

`topics.csv`, separated by `;`, one row per business item:

| Column | |
|---|---|
| `signatur_ges` | the business number, e.g. `17.5209` |
| `topic_1`, `topic_2` | the topics, most likely first |
| `confidence_1`, `confidence_2` | the classifier's probabilities; empty for the parliamentary services' labels |
| `source` | `ogd` for the parliamentary services' label (`thema_1`, `thema_2`), `classifier` for the classifier's |
| `model` | the model that assigned the topics |
| `dok_laufnr` | the document the classifier read |
| `assigned_at` | when the topics were assigned |

Each run classifies the items that are new or not yet classified by the current model.

## Current model

`20261124T161617`, trained on 24 November 2026 at 16:16:17 UTC.

| | |
|---|---|
| Classifier | `LogisticRegression` from scikit-learn 1.9.0: `C = 4`, `solver = "lbfgs"`, `max_iter = 2000`, `class_weight = "balanced"` |
| Embedding | [`Qwen/Qwen3-Embedding-4B`](https://huggingface.co/Qwen/Qwen3-Embedding-4B), vectors scaled to unit length |
| Document | per item the first whose title starts with `Text `, `Text`, `Anzug`, `Motion`, `Interpellation`, `Schriftliche Anfrage`, then the lowest `dok_laufnr` |
| Input text | page markers removed, first 8000 characters. In training the item's title (`titel` in 100086) and a blank line come before the text; in classification only the text |
| Training data | 2437 items with `thema_1` in 100086, begun January 2019 to March 2024, 8 to 130 per topic; the items and documents of the snapshot of 22 September 2026, embedded on 24 November 2026 |
| Selection of `C` | grid 1, 2, 4, 8, 16 with 4-fold cross-validation on 1949 items; test set a random fifth (n = 488, `random_state = 20260101`); the stored model is fit on all 2437 |
| Training code | `topic_classify` in DCC-BS's grossrat-bs-mcp, commit `e24303b` |

| Accuracy (95 % interval) | First topic | First two topics |
|---|---|---|
| Test set (n = 488) | 66.6 % (62.3–70.6) | 82.4 % (78.7–85.5) |
| Cross-validation (n = 1949, 4 folds) | 64.6 % (62.4–66.7) | 81.8 % (80.1–83.5) |

A topic counts as right if it is the parliamentary services' `thema_1`.
