import locale
import logging
import os
import re
from datetime import datetime

import common
import pandas as pd

locale.setlocale(locale.LC_TIME, "de_CH.UTF-8")


def main():
    df_all = merge_dataframes()
    df_all = df_all.dropna(subset=["Datum"])
    df_all["Saison"] = df_all["Saison"] = df_all["Datum"].apply(
        lambda x: f"{x.year}/{x.year + 1}" if x.month >= 7 else f"{x.year - 1}/{x.year}"
    )
    df_all["Tag der Saison"] = df_all["Datum"].apply(calculate_saison_tag)
    df_all["Datum"] = df_all["Datum"].dt.strftime("%Y-%m-%d")
    path_export_file = os.path.join("data", "export", "100302.csv")
    df_all.to_csv(path_export_file, index=False)
    common.update_ftp_and_odsp(path_export_file, "gd_kantonslabor/abwassermonitoring", "100302")


def calculate_saison_tag(datum):
    # Determine season start (always July 1st)
    saison_start = pd.Timestamp(year=datum.year if datum.month >= 7 else datum.year - 1, month=7, day=1)
    tag_nr = (datum - saison_start).days + 1
    if not datum.is_leap_year and datum.month in [3, 4, 5, 6]:
        tag_nr += 1
    return f"Tag {tag_nr:03d} - {datum.strftime('%d. %B')}"


def make_column_dt(df, column):
    df[column] = pd.to_datetime(df[column])


def make_dataframe_bl():
    logging.info("import and transform BL data")
    path = os.path.join("data", "Falldaten-BL", "Abwassermonitoring_Influenza.csv")
    last_changed = os.path.getmtime(path)
    last_changed = str(datetime.fromtimestamp(last_changed).date())
    df_bl = pd.read_csv(path, encoding="ISO-8859-1")
    # remove Gemeinde
    df_bl.drop(columns=["Gemeinde"], inplace=True)
    # add suffix BL for all but date column
    df_bl = df_bl.add_suffix("_BL")
    df_bl.rename(columns={"Testdatum_BL": "Datum"}, inplace=True)
    # make datetime column
    make_column_dt(df_bl, "Datum")
    # sum over date to get total of all together per date
    df_bl = df_bl.groupby(by=["Datum"], as_index=False).sum()
    return df_bl, last_changed


def make_dataframe_bs():
    logging.info("import and transform BS data")
    path = os.path.join("data", "Falldaten-BS", "ISM_export_influenza.xlsx")
    df_bs = pd.read_excel(path)
    last_changed = os.path.getmtime(path)
    last_changed = str(datetime.fromtimestamp(last_changed).date())
    # only keep columns Testdatum [Benötigte Angaben], Serotyp/Subtyp [Erreger]
    df_bs = df_bs[["Testdatum [Benötigte Angaben]", "Serotyp/Subtyp [Erreger]"]]
    new_names = {
        "Testdatum [Benötigte Angaben]": "Datum",
        "Serotyp/Subtyp [Erreger]": "Type",
    }
    df_bs.rename(columns=new_names, inplace=True)
    df_bs = df_bs.pivot_table(index="Datum", columns="Type", aggfunc="size", fill_value=0)
    df_bs.columns.name = None
    df_bs = df_bs.add_prefix("Anz_pos_")
    df_bs = df_bs.add_suffix("_BS")
    df_bs.rename(columns={"Anz_pos_Datum_BS": "Datum"}, inplace=True)
    return df_bs, last_changed


def make_dataframe_abwasser():
    logging.info("import and transform sewage data")
    path = os.path.join("data", "Abwasserdaten", "Probenraster CoroWWmonitoring.xlsx")
    df_abwasser = pd.read_excel(path, header=2, usecols="A,B,F:AB,AP")
    logging.info("Remove text from numerical columns")
    numerical_columns = [
        "InfA (gc/PCR)",
        "InfB (gc/PCR)",
        "InfA (gc/PCR)2",
        "InfB (gc/PCR)2",
        "RSV (gc/PCR)",
        "InfA (gc/L)",
        "InfB (gc/L)",
        "RSV (gc/L)",
        "InfA (gc/L) 7-d median",
        "InfB (gc/L) 7-d median",
        "RSV (gc/L) 7-d median",
        "InfA (gc /100'000 P)",
        "InfB (gc/100'000 P)",
        "RSV (gc /100'000 P)",
        "InfA (gc/100'000 P) 7-d median",
        "InfB (gc/100'000 P) 7-d median",
        "RSV (gc/100'000 P) 7-d median",
        "InfA (gc/PMMoV)",
        "InfB (gc/PMMoV)",
        "RSV (gc /PMMoV)",
        "InfA (gc/PMMoV) 7-d median",
        "InfB (gc/PMMoV) 7-d median",
        "RSV (gc/PMMoV) 7-d median",
        "monthly RSV cases (USB/UKBB, in- & outpatients) ",
    ]
    for column in numerical_columns:
        df_abwasser[column] = pd.to_numeric(df_abwasser[column], errors="coerce")
    return df_abwasser


def add_all_dates(df_bs, date_bs, df_bl, date_bl):
    logging.info("add all dates so that 7d median will be calculated for all")
    updated_until = max(date_bl, date_bs)
    df_bs = df_bs[df_bs.index <= updated_until]
    df_bl = df_bl[df_bl["Datum"] <= updated_until]
    df_infl = pd.merge(df_bs, df_bl, on="Datum", how="outer")
    date_start = str(df_infl["Datum"].min().date())
    date_range = pd.date_range(start=date_start, end=updated_until, freq="D")
    df_infl = df_infl.set_index("Datum")
    df_infl = df_infl.reindex(date_range)
    df_infl = df_infl.reset_index()
    df_infl = df_infl.rename(columns={"index": "Datum"})
    return df_infl


def make_df_infl_bs_bl():
    df_bs, date_bs = make_dataframe_bs()
    df_bl, date_bl = make_dataframe_bl()
    df_infl = add_all_dates(df_bs, date_bs, df_bl, date_bl)
    df_infl = calculate_columns(df_infl)
    return df_infl

KW_PATTERN = re.compile(r"^\s*(\d{4})\s*[-_ ]?\s*K?W?\s*(\d{1,2})\s*$", re.IGNORECASE)

def _normalize_kw(kw: str) -> str:
    """
    Parse various forms like '2024-KW35', '2024_35', '2024 KW 5', '2024-5' -> '2024_05'
    """
    m = KW_PATTERN.match(str(kw))
    if not m:
        raise ValueError(f"Unrecognized KW format: {kw!r}")
    year, week = int(m.group(1)), int(m.group(2))
    return f"{year}_{week:02d}"

def make_dataframe_rsv():
    # --- fortlaufend: count cases per KW ---
    path_fortlaufend = os.path.join("data", "Falldaten-BS", "RSV_Nachweise_USB-fortlaufend ab 2024.csv")
    df_fort = pd.read_csv(path_fortlaufend, sep=";")
    df_fort = df_fort.rename(columns={"DATUM_RSV_NACHWEIS_KALENDERWOCHE": "KW"})["KW"].astype(str)
    df_fort = df_fort.to_frame()
    df_fort["KW_norm"] = df_fort["KW"].apply(_normalize_kw)
    df_fort = df_fort["KW_norm"].value_counts().rename_axis("KW_norm").reset_index(name="KW_Anz_pos_RSV_USB")

    # --- retrospektiv: already weekly counts; normalize KW too ---
    path_retro = os.path.join("data", "Falldaten-BS", "2021-2023 Retrospektive Daten_RSV_Nachweise_USB.xlsx")
    df_retro = pd.read_excel(path_retro)
    # Expect a KW-like column; try common variants
    kw_col = next(c for c in df_retro.columns if c.strip().lower().startswith("kw"))
    df_retro = df_retro.rename(columns={"RSV positiv (Anzahl)": "KW_Anz_pos_RSV_USB", kw_col: "KW"})
    df_retro["KW_norm"] = df_retro["KW"].astype(str).apply(_normalize_kw)
    df_retro = df_retro[["KW_norm", "KW_Anz_pos_RSV_USB"]]

    # --- combine and sum any overlaps ---
    df_weekly = (
        pd.concat([df_retro, df_fort], ignore_index=True)
        .groupby("KW_norm", as_index=False)["KW_Anz_pos_RSV_USB"].sum()
    )

    # --- build full KW span and fill gaps with zeros ---
    # Derive Monday dates for each KW via ISO calendar (weekday=1..7)
    iso = df_weekly["KW_norm"].str.split("_", expand=True).astype(int)
    iso.columns = ["iso_year", "iso_week"]
    # Create a weekly (Monday) anchor date from ISO year/week
    anchor = pd.to_datetime(iso["iso_year"].astype(str) + "-W" + iso["iso_week"].astype(str).str.zfill(2) + "-1",
                            format="%G-W%V-%u")
    # Generate full Monday range and map back to KW_norm
    monday_full = pd.date_range(anchor.min(), anchor.max(), freq="W-MON")
    kz = pd.DataFrame({
        "KW_norm": (monday_full.isocalendar().year.astype(str) + "_" +
                    monday_full.isocalendar().week.astype(str).str.zfill(2)).values
    }).drop_duplicates()

    # Left-join to complete all KWs, fill missing with 0
    df_weekly_full = kz.merge(df_weekly, on="KW_norm", how="left").fillna({"KW_Anz_pos_RSV_USB": 0})
    df_weekly_full["KW_Anz_pos_RSV_USB"] = df_weekly_full["KW_Anz_pos_RSV_USB"].astype(int)

    # --- expand to 7 days per KW and compute dates using ISO strings ---
    df_rsv = df_weekly_full.loc[df_weekly_full.index.repeat(7)].reset_index(drop=True)
    df_rsv["weekday"] = df_rsv.groupby("KW_norm").cumcount() + 1  # ISO weekday: 1..7
    # Build ISO date string 'YYYY-WW-d' and parse
    y = df_rsv["KW_norm"].str.split("_", expand=True)[0]
    w = df_rsv["KW_norm"].str.split("_", expand=True)[1]
    df_rsv["Datum"] = pd.to_datetime(y + "-W" + w + "-" + df_rsv["weekday"].astype(str), format="%G-W%V-%u")

    # Keep a simple 'KW' column with week number
    df_rsv["KW"] = w

    # 7-day mean
    df_rsv["7t_mean_RSV"] = df_rsv["KW_Anz_pos_RSV_USB"] / 7.0

    # Cleanup
    df_rsv = df_rsv.drop(columns=["weekday"])
    # Final column order (KW/week number retained for compatibility)
    df_rsv = df_rsv[["KW", "Datum", "KW_Anz_pos_RSV_USB", "7t_mean_RSV"]]

    return df_rsv

def merge_dataframes():
    df_infl = make_df_infl_bs_bl()
    df_rsv = make_dataframe_rsv()
    df_abwasser = make_dataframe_abwasser()
    merged_df = pd.merge(df_abwasser, df_infl, on="Datum", how="outer")
    merged_df = pd.merge(merged_df, df_rsv, on="Datum", how="left")
    return merged_df


def calculate_columns(df):
    df["InfA_BS+BL"] = df["Anz.pos.A_BL"].fillna(0) + df["Anz_pos_A_BS"].fillna(0)
    df.loc[df["Anz.pos.A_BL"].isna() & df["Anz_pos_A_BS"].isna(), "InfA_BS+BL"] = None
    df["InfB_BS+BL"] = df["Anz.pos.B_BL"].fillna(0) + df["Anz_pos_B_BS"].fillna(0)
    df.loc[df["Anz.pos.B_BL"].isna() & df["Anz_pos_B_BS"].isna(), "InfB_BS+BL"] = None
    df["7t_median_InfA"] = df["InfA_BS+BL"].rolling(window=7, min_periods=1).median()
    df["7t_median_InfB"] = df["InfB_BS+BL"].rolling(window=7, min_periods=1).median()
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
