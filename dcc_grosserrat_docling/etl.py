import logging

import docling_corpus
import pandas as pd
import truststore

truststore.inject_into_ssl()


def main():
    df_dok = pd.read_csv("data_orig/100313_gr_dokumente.csv")
    df_dok.loc[df_dok["url_dok"] == "ohne", "url_dok"] = None
    df_sessionen = pd.read_csv("data_orig/100348_gr_traktanden.csv")[["tag1", "url_vollprotokoll"]].drop_duplicates()
    docling_corpus.run(df_dok, df_sessionen)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    main()
