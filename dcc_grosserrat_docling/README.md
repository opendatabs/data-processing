# Grosser Rat: docling corpus

ETL job run by the Airflow DAG
[dcc_grosserrat_ki](https://github.com/opendatabs/dags-airflow2/blob/main/dcc_grosserrat_ki.py).
It converts the PDFs of all documents ([100313](https://data.bs.ch/explore/dataset/100313)) and all
full session transcripts ([100348](https://data.bs.ch/explore/dataset/100348)) with docling-serve.

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## Output

Each PDF gives three outputs:

- `data/markdown/`: the text as Markdown, with a marker at each page break
- `data/docling/`: docling's full result as JSON
- `data/images/`: the pictures in the PDF

Documents are named by their `dok_laufnr`, transcripts by their session date. Each run converts
only the PDFs that are not converted yet.

## Files

| In the container | | |
|---|---|---|
| `data_orig/100313_gr_dokumente.csv`, `data_orig/100348_gr_traktanden.csv` | `parlamentsdienst_grosserrat/data/export/` (read-only) | the open data |
| `data/markdown/`, `data/docling/`, `data/images/` | `/mnt/OGD-DataExch/StatA/Parlament/` | the output |

## Environment

| Variable | Meaning |
|---|---|
| `DOCLING_HTTP_CLIENT` | Address of docling-serve (DCC's image `ghcr.io/dcc-bs/dcc-docling-serve`) |
| `DOCLING_API_KEY` | API key for docling-serve |

## Errors

- If docling-serve cannot be reached, the job fails before it converts anything.
- If a single PDF fails, the job carries on and tries it again in the next run, three times at most.
- A PDF that no longer exists on grosserrat.bs.ch is handled the same way, but never fails the job.
