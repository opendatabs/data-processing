import logging
import os

import common
import pandas as pd

ALTERSGRUPPEN = [
    "18-24 Jahre",
    "25-29 Jahre",
    "30-39 Jahre",
    "40-49 Jahre",
    "50-59 Jahre",
    "60 Jahre und älter",
]


def main():
    """ Do not run, since it is in the past
    df_rr = process_regierungsrat(which="RR_2_WG")
    path_export = os.path.join(
        "data", "100405_kandidaturen_wahlgang2_regierungsrat.csv"
    )
    df_rr.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/rr/kandidaturen_2024", "100405"
    )

    # Grossratswahlen 2024
    df_gr_2024 = process_grossrat()
    path_export = os.path.join("data", "export", "100385_kandidaturen_grossrat.csv")
    df_gr_2024.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/gr/kandidaturen_2024", "100385"
    )

    # Grossratswahlen 2020
    df_gr_2020 = process_grossrat_2020()
    df_gr_2020["wahljahr"] = "2020"
    df_gr_2024["wahljahr"] = "2024"
    df_gr_alle = pd.concat([df_gr_2020, df_gr_2024])
    path_export = os.path.join("data", "export", "100390_kandidaturen_grossrat.csv")
    df_gr_alle.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/gr/kandidaturen_2024", "100390"
    )

    # Altersgruppen Grossrat
    df_altersgruppen = calculate_altersgruppen(df_gr_2020, df_gr_2024)
    path_export = os.path.join("data", "export", "100392_altersgruppen_grossrat.csv")
    df_altersgruppen.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/gr/kandidaturen_2024", "100392"
    )
    # Häufigkeit Kandidaturen
    df_haeufigkeit = calculate_haeufigkeit()
    path_export = os.path.join("data", "export", "100393_haeufigkeit_kandidaturen.csv")
    df_haeufigkeit.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/gr/kandidaturen_2024", "100393"
    )

    # Berufsgruppen Grossrat
    df_berufsgruppen = calculate_berufsgruppen()
    path_export = os.path.join("data", "export", "100394_berufsgruppen_grossrat.csv")
    df_berufsgruppen.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/gr/kandidaturen_2024", "100394"
    )

    # Regierungsratswahlen 2024
    df_rr = process_regierungsrat()
    path_export = os.path.join("data", "100386_kandidaturen_regierungsrat.csv")
    df_rr.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/rr/kandidaturen_2024", "100386"
    )

    # Regierungspräsidiumswahlen 2024
    df_rp = process_regierungsrat(which="RP")
    path_export = os.path.join("data", "100387_kandidaturen_regierungspraesidium.csv")
    df_rp.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/rr/kandidaturen_2024", "100387"
    )

    # Regierungsrat-Ersatzwahl 2024
    df_rr = process_regierungsrat()
    path_export = os.path.join("data", "100333_kandidaturen_regierungsrat_ersatz.csv")
    df_rr.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/rr_ersatz/kandidaturen_2024", "100333"
    )

    # Regierungspräsidium-Ersatzwahl 2024
    df_rp = process_regierungsrat(which="RP")
    path_export = os.path.join(
        "data", "100334_kandidaturen_regierungspraesidium_ersatz.csv"
    )
    df_rp.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/rr_ersatz/kandidaturen_2024", "100334"
    )

    # Nationalratswahlen 2023
    df_nr = process_nationalrat()
    path_export = os.path.join("data", "100316_kandidaturen_nationalrat.csv")
    df_nr.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/nr/kandidaturen_2023", "100316"
    )

    # Ständeratswahlen 2023
    df_sr = process_staenderat()
    path_export = os.path.join("data", "100317_kandidaturen_staenderat.csv")
    df_sr.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(
        path_export, "wahlen_abstimmungen/wahlen/sr/kandidaturen_2023", "100317"
    )
    """


def process_nationalrat() -> pd.DataFrame:
    file_kb = os.path.join("data_orig", "NR_für_KB_ergänzt.xlsx")
    file_mm = os.path.join("data_orig", "NR_Kandidaturen_für_Medienmitteilung.xlsx")

    xlsx_kb = pd.ExcelFile(file_kb)
    xlsx_mm = pd.ExcelFile(file_mm)

    df_all_kand = pd.DataFrame(index=None)
    for list_party in xlsx_kb.sheet_names:
        df_list = pd.read_excel(xlsx_kb, sheet_name=list_party, skiprows=3, dtype=str)
        df_list.columns = [
            "kand_nr",
            "vorname",
            "name",
            "bisher",
            "jahrgang",
            "kurzbeschrieb",
            "wh. in",
            "wh_in",
        ]
        df_with_header = pd.read_excel(xlsx_kb, sheet_name=list_party, dtype=str)
        hlvs = (
            float("NaN")
            if isinstance(df_with_header.iloc[(1, 1)], float)
            else ", ".join(
                x.strip().zfill(2) for x in df_with_header.iloc[(1, 1)].split(",")
            )
        )
        ulvs = (
            float("NaN")
            if isinstance(df_with_header.iloc[(2, 1)], float)
            else ", ".join(
                x.strip().zfill(2) for x in df_with_header.iloc[(2, 1)].split(",")
            )
        )
        for _, row in df_list.iterrows():
            entries_per_kand = {
                "listenbezeichnung": df_with_header.iloc[(0, 0)],
                "hlv_mit": hlvs,
                "hlv_link": float("NaN")
                if isinstance(hlvs, float)
                else f"https://data.bs.ch/explore/dataset/100316/?refine.listen_nr={'&refine.listen_nr='.join([list_party.split('_')[0]] + hlvs.split(', '))}",
                "ulv_mit": ulvs,
                "ulv_link": float("NaN")
                if isinstance(ulvs, float)
                else f"https://data.bs.ch/explore/dataset/100316/?refine.listen_nr={'&refine.listen_nr='.join([list_party.split('_')[0]] + ulvs.split(', '))}",
                "kand_nr": row.kand_nr,
                "bisher": row.bisher,
                "name_vorname": f"{row['name']}, {row.vorname}",
                "name": row["name"],
                "vorname": row.vorname,
                "jahrgang": row.jahrgang,
                "kurzbeschrieb": row.kurzbeschrieb,
                "wh_in": row.wh_in,
            }
            df_all_kand = pd.concat([df_all_kand, pd.DataFrame([entries_per_kand])])

    # For assigning gender, list number and list name
    df_all_kand_gend = pd.DataFrame(index=None)
    for list_party in xlsx_mm.sheet_names:
        df_list = pd.read_excel(xlsx_mm, sheet_name=list_party, dtype=str)
        df_list = df_list.drop(index=0)
        for _, row in df_list.iterrows():
            entries_per_kand = {
                "kand_nr": row["Kand. Nr."],
                "listen_nr": list_party.split("_", 1)[0],
                "listenkurzbezeichnung": "_".join(list_party.split("_", 1)[1:]),
                "geschlecht": row.Geschlecht,
            }
            df_all_kand_gend = pd.concat(
                [df_all_kand_gend, pd.DataFrame([entries_per_kand])]
            )

    df_nr = pd.merge(df_all_kand, df_all_kand_gend, on="kand_nr")
    return df_nr


def process_staenderat() -> pd.DataFrame:
    file_sr = os.path.join("data_orig", "SR_für_KB.xlsx")
    xlsx_sr = pd.ExcelFile(file_sr)

    df_all_kand = pd.DataFrame(index=None)
    for sheet_name in xlsx_sr.sheet_names:
        df_list = pd.read_excel(xlsx_sr, sheet_name=sheet_name, skiprows=4, dtype=str)
        df_list.columns = [
            "listen_nr",
            "name",
            "vorname",
            "bisher",
            "jahrgang",
            "kurzbeschrieb",
        ]
        df_with_header = pd.read_excel(xlsx_sr, sheet_name=sheet_name, dtype=str)
        entries_per_kand = {
            "listen_nr": sheet_name.split("_", 1)[0],
            "listenbezeichnung": df_with_header.iloc[(2, 0)],
            "bisher": df_list.bisher[0],
            "name_vorname": f"{df_list['name'][0]}, {df_list.vorname[0]}",
            "name": df_list["name"][0],
            "vorname": df_list.vorname[0],
            "geschlecht": "f" if sheet_name == "01_Herzog" else "m",
            "jahrgang": df_list.jahrgang[0],
            "kurzbeschrieb": df_list.kurzbeschrieb[0],
        }
        df_all_kand = pd.concat([df_all_kand, pd.DataFrame([entries_per_kand])])

    return df_all_kand


def process_regierungsrat(which="RR"):
    xlsx = os.path.join("data_orig", f"{which}_für_OGD.xlsx")

    df = pd.DataFrame(index=None)
    for sheet_name in pd.ExcelFile(xlsx).sheet_names:
        df_list = pd.read_excel(xlsx, sheet_name=sheet_name, dtype=str)
        df_list.columns = [
            "listen_nr",
            "listenbezeichnung",
            "zeilen_nr",
            "name",
            "vorname",
            "bisher",
            "geschlecht",
            "jahrgang",
            "zusatz",
        ]
        df = pd.concat([df, df_list])
    df["name_vorname"] = df["name"] + ", " + df["vorname"]
    df["listen_nr"] = df["listen_nr"].apply(lambda x: x.zfill(2))
    df = df.drop(columns=["zeilen_nr"])
    return df


def process_grossrat():
    df = pd.DataFrame(index=None)
    wahlkreise = [
        ("GBO", "Grossbasel-Ost"),
        ("GBW", "Grossbasel-West"),
        ("KB", "Kleinbasel"),
        ("RI", "Riehen"),
        ("BE", "Bettingen"),
    ]

    df_berufe = pd.read_excel(os.path.join("data_orig", "Berufe_GR.xlsx"), dtype=str)
    df_berufsgruppen = pd.read_csv(
        os.path.join("data_orig", "Berufsgruppen.csv"), sep=";", dtype=str
    )
    df_berufe = pd.merge(df_berufe, df_berufsgruppen, on="berufscode", how="left")

    for wahlkreis in wahlkreise:
        xlsx = os.path.join("data_orig", f"{wahlkreis[0]}_für_OGD.xlsx")
        for sheet_name in pd.ExcelFile(xlsx).sheet_names:
            df_list = pd.read_excel(xlsx, sheet_name=sheet_name, skiprows=2, dtype=str)
            df_list.columns = [
                "listen_nr",
                "listenkurzbezeichnung",
                "listenbezeichnung",
                "kand_nr",
                "name",
                "vorname",
                "bisher",
                "geschlecht",
                "jahrgang",
                "zusatz",
            ]
            df_list["listen_nr"] = (
                df_list["listen_nr"].astype(str).apply(lambda x: x.zfill(2))
            )
            df_list["wahlkreis"] = wahlkreis[1]
            df_list["name_vorname"] = df_list["name"] + ", " + df_list["vorname"]
            df_list["altersgruppe"] = pd.cut(
                2024 - df_list["jahrgang"].astype(int),
                [17, 24, 29, 39, 49, 59, 100],
                labels=ALTERSGRUPPEN,
            )
            df_beruf_wahlkreis = df_berufe[df_berufe["wahlkreis"] == wahlkreis[1]].drop(
                columns=[
                    "wahljahr",
                    "wahlkreis",
                    "listen_nr",
                    "listenkurzbezeichnung",
                    "name",
                    "vorname",
                    "name_vorname",
                    "geschlecht",
                    "jahrgang",
                    "bisher",
                    "zusatz",
                ]
            )
            df_list = pd.merge(df_list, df_beruf_wahlkreis, on="kand_nr", how="left")
            df = pd.concat([df, df_list])
    return df


def process_grossrat_2020():
    df_resultat = pd.read_csv(
        os.path.join("data_orig", "Schlussresultat2020.csv"), sep=";", dtype=str
    )
    df = df_resultat[
        [
            "Wahlkreisbezeichnung",
            "Listennummer",
            "Parteikurzbezeichnung",
            "Parteibezeichnung",
            "Kandidaten-Nr",
            "Name",
            "Vorname",
            "bisher",
            "Geschlecht",
            "Jahrgang",
            "Beruf",
        ]
    ]
    df = df.rename(
        columns={
            "Wahlkreisbezeichnung": "wahlkreis",
            "Listennummer": "listen_nr",
            "Parteikurzbezeichnung": "listenkurzbezeichnung",
            "Parteibezeichnung": "listenbezeichnung",
            "Kandidaten-Nr": "kand_nr",
            "Name": "name",
            "Vorname": "vorname",
            "Geschlecht": "geschlecht",
            "Jahrgang": "jahrgang",
            "Beruf": "zusatz",
        }
    )
    df["bisher"] = df["bisher"].replace({"nicht amtierend": ""})
    df["kand_nr"] = df["kand_nr"].apply(lambda x: f"{x[:2]}.{x[2:]}")
    df["name_vorname"] = df["name"] + ", " + df["vorname"]
    df["geschlecht"] = df["geschlecht"].replace({"M": "m", "F": "f"})
    df["altersgruppe"] = pd.cut(
        2020 - df["jahrgang"].astype(int),
        [17, 24, 29, 39, 49, 59, 100],
        labels=ALTERSGRUPPEN,
    )
    return df


def calculate_altersgruppen(df_gr_2020, df_gr_2024):
    df_altersgruppen = pd.DataFrame(
        columns=[
            "wahljahr",
            "listenkurzbezeichnung",
            "geschlecht",
            "altersgruppe",
            "anzahl",
            "anteil",
            "anteil_altersgruppe",
            "anteil_liste",
        ]
    )

    for wahljahr in ["2020", "2024"]:
        df = df_gr_2020 if wahljahr == "2020" else df_gr_2024
        anzahl_wahljahr = df.shape[0]

        for liste in df["listenkurzbezeichnung"].unique():
            anzahl_wahljahr_liste = df[df["listenkurzbezeichnung"] == liste].shape[0]

            for geschlecht in ["m", "f"]:
                for altersgruppe in ALTERSGRUPPEN:
                    anzahl_wahljahr_altersgruppe = df[
                        df["altersgruppe"] == altersgruppe
                    ].shape[0]
                    anzahl = df[
                        (df["listenkurzbezeichnung"] == liste)
                        & (df["geschlecht"] == geschlecht)
                        & (df["altersgruppe"] == altersgruppe)
                    ].shape[0]

                    # Ensure that we always have the right number of columns and data matches the structure
                    row = {
                        "wahljahr": wahljahr,
                        "listenkurzbezeichnung": liste,
                        "geschlecht": geschlecht,
                        "altersgruppe": altersgruppe,
                        "anzahl": anzahl,
                        "anteil": anzahl / anzahl_wahljahr,
                        "anteil_altersgruppe": anzahl / anzahl_wahljahr_altersgruppe,
                        "anteil_liste": anzahl / anzahl_wahljahr_liste,
                    }

                    # Append row to DataFrame
                    df_altersgruppen = pd.concat(
                        [df_altersgruppen, pd.DataFrame([row])], ignore_index=True
                    )

    df_frauen = df_altersgruppen[df_altersgruppen["geschlecht"] == "f"]
    df_frauen_sum = (
        df_frauen.groupby(["wahljahr", "listenkurzbezeichnung"])["anteil_liste"]
        .sum()
        .reset_index()
    )
    df_frauen_sum["frauenanteil_liste"] = df_frauen_sum.groupby("wahljahr")[
        "anteil_liste"
    ].rank(ascending=False, method="min")
    # Merge the ranking back into the original DataFrame
    df_altersgruppen = df_altersgruppen.merge(
        df_frauen_sum[["wahljahr", "listenkurzbezeichnung", "frauenanteil_liste"]],
        on=["wahljahr", "listenkurzbezeichnung"],
        how="left",
    )
    df_altersgruppen["frauenanteil_liste"] = (
        df_altersgruppen["frauenanteil_liste"].astype(int).astype(str).str.zfill(2)
        + ". "
        + df_altersgruppen["listenkurzbezeichnung"].astype(str)
    )
    return df_altersgruppen


def calculate_berufsgruppen():
    df_berufsgruppe = pd.DataFrame(
        index=None, columns=["wahljahr", "berufsgruppe", "anzahl", "anteil"]
    )
    df_berufsgruppen_zut = pd.read_csv(
        os.path.join("data_orig", "Berufsgruppen.csv"), sep=";", dtype=str
    )
    for wahljahr in ["2020", "2024"]:
        df_berufe = pd.read_excel(
            os.path.join("data_orig", "Berufe_GR.xlsx"), dtype=str, sheet_name=wahljahr
        )
        df_berufe = df_berufe["berufscode"].value_counts().reset_index()
        df_berufe = pd.merge(
            df_berufe, df_berufsgruppen_zut, on="berufscode", how="left"
        )[["berufsgruppe", "count"]]
        df_berufe = df_berufe.rename(columns={"count": "anzahl"})
        df_berufe["wahljahr"] = wahljahr
        df_berufe["anteil"] = df_berufe["anzahl"] / df_berufe["anzahl"].sum()
        df_berufsgruppe = pd.concat([df_berufsgruppe, df_berufe])
    return df_berufsgruppe


def calculate_haeufigkeit():
    # Load 2024 data
    df_2024 = pd.read_excel(os.path.join("data_orig", "Kand_GR_2024_mit_dmnr.xlsx"))

    # Load historical data
    historical_years = [2020, 2016, 2012, 2008]
    historical_data = {}
    for year in historical_years:
        historical_data[year] = pd.read_excel(
            os.path.join("data_orig", "GR-Kandidaturen-safe.xlsx"), sheet_name=str(year)
        )

    # Initialize result dictionary
    result = {"Häufigkeit": [], "Kandidaturen": [], "Gewählt": [], "Anzahl": []}

    # Iterate over each candidate in 2024
    for index, row in df_2024.iterrows():
        dmnr_2024 = row["dm-nr"]
        candidacies = ["2024"]
        elected = False

        # Check each historical year
        for year in historical_years:
            df_year = historical_data[year]
            candidate = df_year[df_year["dm-nr"] == dmnr_2024]
            if not candidate.empty:
                candidacies.append(str(year))
                if candidate["wahl"].isin([3, 4, 7, 9]).any():
                    elected = True

        # Create candidacies string
        candidacy_str = f"Kandidatur {', '.join(candidacies)}"
        if len(candidacies) == 1:
            candidacy_str = "Erstmals Kandidierende seit 2008"

        # Append results
        result["Häufigkeit"].append(len(candidacies))
        result["Kandidaturen"].append(candidacy_str)
        result["Gewählt"].append("Gewählt" if elected else "Nicht Gewählt")
        result["Anzahl"].append(1)

    # Create result dataframe
    result_df = pd.DataFrame(result)

    # Group by and aggregate
    final_df = (
        result_df.groupby(["Häufigkeit", "Kandidaturen", "Gewählt"]).sum().reset_index()
    )

    # Define all possible combinations of Häufigkeit, Kandidaturen, and Gewählt
    all_combinations = []
    for freq in range(1, 6):  # Häufigkeit can range from 1 to 5
        for years in [
            ["2024"],
            ["2024", "2020"],
            ["2024", "2016"],
            ["2024", "2012"],
            ["2024", "2008"],
            ["2024", "2020", "2016"],
            ["2024", "2020", "2012"],
            ["2024", "2020", "2008"],
            ["2024", "2016", "2012"],
            ["2024", "2016", "2008"],
            ["2024", "2012", "2008"],
            ["2024", "2020", "2016", "2012"],
            ["2024", "2020", "2016", "2008"],
            ["2024", "2020", "2012", "2008"],
            ["2024", "2016", "2012", "2008"],
            ["2024", "2020", "2016", "2012", "2008"],
        ]:
            if len(years) == freq:
                candidacy_str = f"Kandidatur {', '.join(years)}"
                if len(years) == 1:
                    candidacy_str = "Erstmals Kandidierende seit 2008"
                all_combinations.append((freq, candidacy_str, "Nicht Gewählt"))
                all_combinations.append((freq, candidacy_str, "Gewählt"))

    # Create a DataFrame with all combinations
    combinations_df = pd.DataFrame(
        all_combinations, columns=["Häufigkeit", "Kandidaturen", "Gewählt"]
    )

    # Merge final_df with combinations_df to include all possible combinations
    final_df = pd.merge(
        combinations_df,
        final_df,
        how="left",
        on=["Häufigkeit", "Kandidaturen", "Gewählt"],
    )

    # Fill missing values in 'Anzahl' with 0
    final_df["Anzahl"] = final_df["Anzahl"].fillna(0)

    # Drop the extra columns from the merge
    final_df = final_df[["Häufigkeit", "Kandidaturen", "Gewählt", "Anzahl"]]

    return final_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
