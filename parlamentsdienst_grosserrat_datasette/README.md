# Parlamentsdienst Grosser Rat: datasette

ETL job run by the Airflow DAG
[parlamentsdienst_grosserrat_datasette](https://github.com/opendatabs/dags-airflow2/blob/main/parlamentsdienst_grosserrat_datasette.py).
It builds the database `data/datasette/GrosserRat.db` from the open data of the Grosser Rat:

- [100307](https://data.bs.ch/explore/dataset/100307) to [100314](https://data.bs.ch/explore/dataset/100314)
- [100348](https://data.bs.ch/explore/dataset/100348)

The PDFs of the documents and transcripts are converted by [dcc_grosserrat_docling](../dcc_grosserrat_docling).

## Data Owner

Parlamentsdienst des Grossen Rates Basel-Stadt
