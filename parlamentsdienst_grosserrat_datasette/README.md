# Parlamentsdienst Grosser Rat: datasette and docling corpus

ETL job with two parts, run by the Airflow DAG
[parlamentsdienst_grosserrat_datasette](https://github.com/opendatabs/dags-airflow2/blob/main/parlamentsdienst_grosserrat_datasette.py):

1. **datasette** (`etl.py`): builds the database `data/datasette/GrosserRat.db` from the open data
   of the Grosser Rat:
   - [100307](https://data.bs.ch/explore/dataset/100307) to [100314](https://data.bs.ch/explore/dataset/100314)
   - [100348](https://data.bs.ch/explore/dataset/100348)
2. **docling corpus** (`docling_corpus.py`): converts the PDFs of all documents
   ([100313](https://data.bs.ch/explore/dataset/100313)) and all full session transcripts
   ([100348](https://data.bs.ch/explore/dataset/100348)) with docling-serve.

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt

## Output of the docling corpus

Each PDF gives three outputs:

- `data/markdown/`: the text as Markdown, with a marker at each page break
- `data/docling/`: docling's full result as JSON
- `data/images/`: the pictures in the PDF

Documents are named by their `dok_laufnr`, transcripts by their session date. Each run converts
only the PDFs that are not converted yet.

## Environment

| Variable | Meaning |
|---|---|
| `DOCLING_HTTP_CLIENT` | Address of docling-serve (DCC's image `ghcr.io/dcc-bs/dcc-docling-serve`) |
| `DOCLING_API_KEY` | API key for docling-serve |

## Errors

- If docling-serve cannot be reached, the job fails before it converts anything.
- If a single PDF fails, the job carries on and tries it again in the next run, three times at most.
- A PDF that no longer exists on grosserrat.bs.ch is handled the same way, but never fails the job.
