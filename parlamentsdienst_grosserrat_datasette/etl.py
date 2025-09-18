import logging
import sqlite3
from pathlib import Path

import common
import pandas as pd
import pdf_converter


def safe_converter(func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except Exception as e:
        logging.exception("Converter failed: %s (%s)", func.__name__, e)


def main():
    logging.info("Starting build…")
    db_path = Path("data") / "datasette" / "GrosserRat.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    cur = conn.cursor()

    # ---------- Load CSVs ----------
    df_adr_raw = pd.read_csv("data_orig/100307_gr_mitglieder.csv")
    df_mit = pd.read_csv("data_orig/100308_gr_mitgliedschaften.csv")
    df_int = pd.read_csv("data_orig/100309_gr_interessensbindungen.csv")
    df_gre = pd.read_csv("data_orig/100310_gr_gremien.csv")
    df_ges = pd.read_csv("data_orig/100311_gr_geschaefte.csv")
    df_zuw = pd.read_csv("data_orig/100312_gr_zuweisungen.csv")
    df_dok_full = pd.read_csv("data_orig/100313_gr_dokumente.csv")
    df_vor = pd.read_csv("data_orig/100314_gr_vorgaenge.csv")
    df_tag_trakt = pd.read_csv("data_orig/100348_gr_traktanden.csv")
    """
    df_unt = common.pandas_read_csv(
        "https://grosserrat.bs.ch/index.php?option=com_gribs&view=exporter&format=csv&chosentable=unt",
        encoding="utf-8",
        dtype=str,
    )
    """

    # --------- Drop in FK-safe order ---------
    for t in [
        "Traktanden",
        "Tagesordnungen",
        "Unterlagen",
        "Sessionen",
        "Sitzungen",
        "Vorgaenge",
        "Dokumente",
        "Zuweisungen",
        "Geschaefte",
        "Mitgliedschaften",
        "Gremien",
        "Interessensbindungen",
        "Personen",
    ]:
        cur.execute(f'DROP TABLE IF EXISTS "{t}"')

    # --------- Personen (ADR) ---------
    logging.info("Creating table for Personen…")
    personen_cols = [
        "uni_nr",
        "ist_aktuell_grossrat",
        "anrede",
        "titel",
        "name",
        "vorname",
        "name_vorname",
        "gebdatum",
        "url",
        "strasse",
        "plz",
        "ort",
        "gr_beruf",
        "gr_arbeitgeber",
        "gr_sitzplatz",
        "gr_wahlkreis",
        "partei",
        "partei_kname",
        "homepage",
    ]
    df_personen = df_adr_raw.sort_values(by=["gr_beginn"]).drop_duplicates(subset=["uni_nr"])[personen_cols]

    cur.execute("""
        CREATE TABLE "Personen" (
            "uni_nr" INTEGER PRIMARY KEY,
            "ist_aktuell_grossrat" TEXT,
            "anrede" TEXT,
            "titel" TEXT,
            "name" TEXT,
            "vorname" TEXT,
            "name_vorname" TEXT,
            "gebdatum" TEXT,
            "url" TEXT,
            "strasse" TEXT,
            "plz" INTEGER,
            "ort" TEXT,
            "gr_beruf" TEXT,
            "gr_arbeitgeber" TEXT,
            "gr_sitzplatz" INTEGER,
            "gr_wahlkreis" TEXT,
            "partei" TEXT,
            "partei_kname" TEXT,
            "homepage" TEXT
        )
    """)
    df_personen.to_sql("Personen", conn, if_exists="append", index=False)
    common.create_indices(
        conn,
        "Personen",
        [
            "ist_aktuell_grossrat",
            "anrede",
            "name",
            "vorname",
            "gebdatum",
            "strasse",
            "plz",
            "ort",
            "gr_beruf",
            "gr_wahlkreis",
            "partei",
            "partei_kname",
        ],
    )

    # --------- Gremien ---------
    logging.info("Creating table for Gremien…")
    cur.execute("""
        CREATE TABLE "Gremien" (
            "uni_nr" INTEGER PRIMARY KEY,
            "ist_aktuelles_gremium" TEXT,
            "kurzname" TEXT,
            "name" TEXT,
            "gremientyp" TEXT
        )
    """)
    df_gre = df_gre[["uni_nr", "ist_aktuelles_gremium", "kurzname", "name", "gremientyp"]]
    df_gre.to_sql("Gremien", conn, if_exists="append", index=False)
    common.create_indices(conn, "Gremien", ["ist_aktuelles_gremium", "kurzname", "name", "gremientyp"])

    # --------- Mitgliedschaften (MIT) ---------
    logging.info("Creating table for Mitgliedschaften…")
    cur.execute("""
        CREATE TABLE "Mitgliedschaften" (
            "uni_nr_gre" INTEGER,
            "uni_nr_adr" INTEGER,
            "beginn_mit" TEXT,
            "ende_mit" TEXT,
            "funktion_adr" TEXT,
            FOREIGN KEY ("uni_nr_gre") REFERENCES "Gremien"("uni_nr") ON DELETE CASCADE,
            FOREIGN KEY ("uni_nr_adr") REFERENCES "Personen"("uni_nr") ON DELETE CASCADE
        )
    """)
    df_mit = df_mit[["uni_nr_gre", "uni_nr_adr", "beginn_mit", "ende_mit", "funktion_adr"]]
    df_mit.to_sql("Mitgliedschaften", conn, if_exists="append", index=False)
    common.create_indices(
        conn, "Mitgliedschaften", ["uni_nr_gre", "beginn_mit", "ende_mit", "funktion_adr", "uni_nr_adr"]
    )

    # --------- Interessensbindungen (IBI) ---------
    logging.info("Creating table for Interessensbindungen…")
    cur.execute("""
        CREATE TABLE "Interessensbindungen" (
            "rubrik" TEXT,
            "intr-bind" TEXT,
            "funktion" TEXT,
            "text" TEXT,
            "uni_nr" INTEGER,
            FOREIGN KEY ("uni_nr") REFERENCES "Personen"("uni_nr") ON DELETE CASCADE
        )
    """)
    df_int = df_int[["rubrik", "intr-bind", "funktion", "text", "uni_nr"]]
    df_int.to_sql("Interessensbindungen", conn, if_exists="append", index=False)
    common.create_indices(conn, "Interessensbindungen", ["rubrik", "intr-bind", "funktion", "uni_nr"])

    # --------- Geschaefte ---------
    logging.info("Creating table for Geschaefte…")
    cur.execute("""
        CREATE TABLE "Geschaefte" (
            "laufnr_ges" INTEGER PRIMARY KEY,
            "beginn_ges" TEXT,
            "ende_ges" TEXT,
            "signatur_ges" TEXT,
            "status_ges" TEXT,
            "titel_ges" TEXT,
            "departement_ges" TEXT,
            "ga_rr_gr" TEXT,
            "url_ges" TEXT,
            "nr_urheber_person" INTEGER,
            "nr_urheber_gremium" INTEGER,
            "nr_miturheber_person" INTEGER,
            "nr_miturheber_gremium" INTEGER,
            FOREIGN KEY ("nr_urheber_person") REFERENCES Personen(uni_nr),
            FOREIGN KEY ("nr_urheber_gremium") REFERENCES Gremien(uni_nr),
            FOREIGN KEY ("nr_miturheber_person") REFERENCES Personen(uni_nr),
            FOREIGN KEY ("nr_miturheber_gremium") REFERENCES Gremien(uni_nr)
        );
    """)
    # make IDs numeric
    for col in ["nr_urheber", "nr_miturheber"]:
        df_ges[col] = pd.to_numeric(df_ges[col], errors="coerce")

    # initialize split columns
    df_ges["nr_urheber_person"] = pd.NA
    df_ges["nr_urheber_gremium"] = pd.NA
    df_ges["nr_miturheber_person"] = pd.NA
    df_ges["nr_miturheber_gremium"] = pd.NA

    # classify urheber
    mask_person = df_ges["anrede_urheber"].notna() & (df_ges["url_urheber_ratsmitgl"].notna())
    mask_gremium = df_ges["gremientyp_urheber"].notna()
    df_ges.loc[mask_person, "nr_urheber_person"] = df_ges.loc[mask_person, "nr_urheber"]
    df_ges.loc[mask_gremium & ~mask_person, "nr_urheber_gremium"] = df_ges.loc[
        mask_gremium & ~mask_person, "nr_urheber"
    ]

    # classify miturheber
    mask_person = df_ges["anrede_miturheber"].notna() & (df_ges["url_miturheber_ratsmitgl"].notna())
    mask_gremium = df_ges["gremientyp_miturheber"].notna()
    df_ges.loc[mask_person, "nr_miturheber_person"] = df_ges.loc[mask_person, "nr_miturheber"]
    df_ges.loc[mask_gremium & ~mask_person, "nr_miturheber_gremium"] = df_ges.loc[
        mask_gremium & ~mask_person, "nr_miturheber"
    ]

    # final column selection
    df_ges = df_ges[
        [
            "laufnr_ges",
            "beginn_ges",
            "ende_ges",
            "signatur_ges",
            "status_ges",
            "titel_ges",
            "departement_ges",
            "ga_rr_gr",
            "url_ges",
            "nr_urheber_person",
            "nr_urheber_gremium",
            "nr_miturheber_person",
            "nr_miturheber_gremium",
        ]
    ].drop_duplicates("laufnr_ges")

    df_ges.to_sql("Geschaefte", conn, if_exists="append", index=False)
    common.create_indices(
        conn,
        "Geschaefte",
        [
            "beginn_ges",
            "ende_ges",
            "status_ges",
            "departement_ges",
            "ga_rr_gr",
            "nr_urheber_person",
            "nr_urheber_gremium",
            "nr_miturheber_person",
            "nr_miturheber_gremium",
        ],
    )

    # --------- Zuweisungen ---------
    logging.info("Creating table for Zuweisungen…")
    cur.execute("""
        CREATE TABLE "Zuweisungen" (
            "id" INTEGER PRIMARY KEY,
            "uni_nr_an" INTEGER,
            "erledigt" TEXT,
            "laufnr_ges" INTEGER,
            "status_zuw" TEXT,
            "termin" TEXT,
            "titel_zuw" TEXT,
            "bem" TEXT,
            "uni_nr_von" INTEGER,
            FOREIGN KEY ("laufnr_ges") REFERENCES "Geschaefte"("laufnr_ges") ON DELETE CASCADE,
            FOREIGN KEY ("uni_nr_an") REFERENCES "Gremien"("uni_nr"),
            FOREIGN KEY ("uni_nr_von") REFERENCES "Gremien"("uni_nr")
        )
    """)
    df_zuw["uni_nr_an"] = df_zuw["uni_nr_an"].where(df_zuw["url_gremium_an"].notna(), pd.NA)
    df_zuw["uni_nr_von"] = df_zuw["uni_nr_von"].where(df_zuw["url_gremium_von"].notna(), pd.NA)
    df_zuw = df_zuw[["uni_nr_an", "erledigt", "laufnr_ges", "status_zuw", "termin", "titel_zuw", "bem", "uni_nr_von"]]
    df_zuw.to_sql("Zuweisungen", conn, if_exists="append", index=False)
    common.create_indices(conn, "Zuweisungen", ["uni_nr_an", "erledigt", "status_zuw", "uni_nr_von", "laufnr_ges"])

    # --------- Dokumente ---------
    logging.info("Creating table for Dokumente…")
    cur.execute("""
        CREATE TABLE "Dokumente" (
            "dok_laufnr" INTEGER PRIMARY KEY,
            "dokudatum" TEXT,
            "titel_dok" TEXT,
            "url_dok" TEXT,
            "signatur_dok" TEXT,
            "laufnr_ges" INTEGER,
            FOREIGN KEY ("laufnr_ges") REFERENCES "Geschaefte"("laufnr_ges") ON DELETE SET NULL
        )
    """)
    df_dok = df_dok_full[["dok_laufnr", "dokudatum", "titel_dok", "url_dok", "signatur_dok", "laufnr_ges"]]
    df_dok.to_sql("Dokumente", conn, if_exists="append", index=False)
    common.create_indices(conn, "Dokumente", ["dokudatum", "titel_dok", "laufnr_ges"])

    # --------- Vorgaenge & Sitzungen ---------
    logging.info("Creating table for Sitzungen…")
    cur.execute("""
        CREATE TABLE "Sitzungen" (
            "siz_nr" INTEGER PRIMARY KEY,
            "siz_datum" TEXT
        )
    """)
    df_sitzungen = df_vor[["siz_nr", "siz_datum"]].drop_duplicates().copy()
    df_sitzungen.to_sql("Sitzungen", conn, if_exists="append", index=False)

    logging.info("Creating tables for Vorgaenge…")
    cur.execute("""
        CREATE TABLE "Vorgaenge" (
            "nummer" INTEGER,
            "siz_nr" INTEGER,
            "beschlnr" TEXT,
            "Vermerk" TEXT,
            "laufnr_ges" INTEGER,
            FOREIGN KEY ("laufnr_ges") REFERENCES "Geschaefte"("laufnr_ges") ON DELETE CASCADE,
            FOREIGN KEY ("siz_nr") REFERENCES "Sitzungen"("siz_nr") ON DELETE CASCADE
        )
    """)
    df_vor = df_vor[["nummer", "siz_nr", "beschlnr", "Vermerk", "laufnr_ges"]]
    df_vor.to_sql("Vorgaenge", conn, if_exists="append", index=False)
    common.create_indices(conn, "Vorgaenge", ["Vermerk", "siz_nr", "laufnr_ges"])

    # --------- Sessionen & Tagesordnungen & Traktanden ---------
    logging.info("Creating table for Sessionen…")
    cur.execute("""
        CREATE TABLE "Sessionen" (
            "gr_sitzung_idnr" INTEGER PRIMARY KEY,
            "tagesordnung_idnr" INTEGER,
            "versand" TEXT,
            "tag1" TEXT,
            "text1" TEXT,
            "tag2" TEXT,
            "text2" TEXT,
            "tag3" TEXT,
            "text3" TEXT,
            "bemerkung" TEXT,
            "protokollseite_von" INTEGER,
            "protokollseite_bis" INTEGER,
            "url_vollprotokoll" TEXT,
            "url_audioprotokoll_tag1" TEXT,
            "url_audioprotokoll_tag2" TEXT,
            "url_audioprotokoll_tag3" TEXT
        )
    """)
    # Tagesordnungen PDFs come from Sessionen (url_vollprotokoll)
    df_sessionen_src = df_tag_trakt[
        [
            "gr_sitzung_idnr",
            "tagesordnung_idnr",
            "versand",
            "tag1",
            "text1",
            "tag2",
            "text2",
            "tag3",
            "text3",
            "bemerkung",
            "protokollseite_von",
            "protokollseite_bis",
            "url_vollprotokoll",
            "url_audioprotokoll_tag1",
            "url_audioprotokoll_tag2",
            "url_audioprotokoll_tag3",
        ]
    ].drop_duplicates()
    df_sessionen_src.to_sql("Sessionen", conn, if_exists="append", index=False)

    logging.info("Creating tables for Tagesordnungen...")
    cur.execute("""
        CREATE TABLE "Tagesordnungen" (
            "tagesordnung_idnr" INTEGER PRIMARY KEY,
            "gr_sitzung_idnr" INTEGER,
            "einleitungstext" TEXT,
            "zwischentext" TEXT,
            "url_tagesordnung_dok" TEXT,
            "url_geschaeftsverzeichnis" TEXT,
            "url_sammelmappe" TEXT,
            "url_alle_dokumente" TEXT,
            FOREIGN KEY ("gr_sitzung_idnr") REFERENCES "Sessionen"("gr_sitzung_idnr") ON DELETE CASCADE
        )
    """)
    df_tagesordnung = df_tag_trakt[
        [
            "tagesordnung_idnr",
            "gr_sitzung_idnr",
            "einleitungstext",
            "zwischentext",
            "url_tagesordnung_dok",
            "url_geschaeftsverzeichnis",
            "url_sammelmappe",
            "url_alle_dokumente",
        ]
    ].drop_duplicates()
    df_tagesordnung.to_sql("Tagesordnungen", conn, if_exists="append", index=False)

    logging.info("Creating table for Traktanden…")
    cur.execute("""
        CREATE TABLE "Traktanden" (
            "traktanden_idnr" INTEGER PRIMARY KEY,
            "tagesordnung_idnr" INTEGER,
            "gruppennummer" INTEGER,
            "gruppentitel" TEXT,
            "gruppentitel_pos" INTEGER,
            "laufnr" INTEGER,
            "laufnr_2" REAL,
            "status" TEXT,
            "titel" TEXT,
            "kommission" TEXT,
            "departement" TEXT,
            "signatur" TEXT,
            "Abstimmung" TEXT,
            "anr" TEXT,
            FOREIGN KEY ("tagesordnung_idnr") REFERENCES "Tagesordnungen"("tagesordnung_idnr") ON DELETE CASCADE
        )
    """)
    df_trakt = df_tag_trakt[
        [
            "traktanden_idnr",
            "tagesordnung_idnr",
            "gruppennummer",
            "gruppentitel",
            "gruppentitel_pos",
            "laufnr",
            "laufnr_2",
            "status",
            "titel",
            "kommission",
            "departement",
            "signatur",
            "Abstimmung",
            "anr",
        ]
    ]
    df_trakt.to_sql("Traktanden", conn, if_exists="append", index=False)
    common.create_indices(
        conn,
        "Traktanden",
        [
            "tagesordnung_idnr",
            "gruppennummer",
            "gruppentitel",
            "gruppentitel_pos",
            "laufnr",
            "laufnr_2",
            "status",
            "kommission",
            "departement",
            "signatur",
        ],
    )

    '''
    # --------- Unterlagen (with indices) ---------
    logging.info("Creating table for Unterlagen…")
    cur.execute("""
        CREATE TABLE "Unterlagen" (
            "idnr" INTEGER PRIMARY KEY,
            "beschluss" INTEGER,
            "dok_nr" INTEGER,
            "siz_nr" INTEGER,
            FOREIGN KEY ("siz_nr") REFERENCES "Sitzungen"("siz_nr") ON DELETE CASCADE,
            FOREIGN KEY ("dok_nr") REFERENCES "Dokumente"("dok_laufnr") ON DELETE SET NULL
        )
    """)
    df_unt = df_unt.rename(columns={c: c.strip() for c in df_unt.columns})
    for col in ["idnr", "beschluss", "dok_nr", "siz_nr"]:
        if col in df_unt.columns:
            df_unt[col] = pd.to_numeric(df_unt[col], errors="coerce")
        else:
            df_unt[col] = pd.NA
    df_unt = df_unt[["idnr", "beschluss", "dok_nr", "siz_nr"]].drop_duplicates()
    df_unt.to_sql("Unterlagen", conn, if_exists="append", index=False)
    common.create_indices(conn, "Unterlagen", ["siz_nr"])
    common.create_indices(conn, "Unterlagen", ["dok_nr"])
    common.create_indices(conn, "Unterlagen", ["beschluss"])
    common.create_indices(conn, "Unterlagen", ["beschluss", "siz_nr"])  # composite
    '''

    conn.commit()
    conn.close()
    logging.info("Build complete.")

    # ---------- Converters (guarded) ----------
    df_dok_copy = df_dok_full.copy()
    df_dok_copy.loc[df_dok_copy["url_dok"] == "ohne", "url_dok"] = None
    
    for method in ["pdfplumber", "pymupdf"]:
        safe_converter(
            pdf_converter.create_text_from_column,
            df_dok_copy,
            "url_dok",
            method,
            Path("data/text") / f"gr_dokumente_text_{method}.zip",
            "dok_laufnr",
        )
    for method in ["docling", "pymupdf", "pymupdf4llm"]:
        safe_converter(
            pdf_converter.create_markdown_from_column,
            df_dok_copy,
            "url_dok",
            method,
            Path("data/markdown") / f"gr_dokumente_md_{method}.zip",
            "dok_laufnr",
        )
    
    for method in ["pdfplumber", "pymupdf"]:
        safe_converter(
            pdf_converter.create_text_from_column,
            df_sessionen_src,
            "url_vollprotokoll",
            method,
            Path("data/text") / f"gr_vollprotokoll_text_{method}.zip",
            "tag1",
        )
    for method in ["docling", "pymupdf", "pymupdf4llm"]:
        safe_converter(
            pdf_converter.create_markdown_from_column,
            df_sessionen_src,
            "url_vollprotokoll",
            method,
            Path("data/markdown") / f"gr_vollprotokoll_md_{method}.zip",
            "tag1",
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
