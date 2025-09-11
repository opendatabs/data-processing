import os

import common
import dateparser
import numpy as np
import pandas as pd


def main():
    data_file_names = ["Resultate_EID.xlsx", "Resultate_KAN.xlsx"]
    abst_date, concatenated_df = calculate_details(data_file_names)

    export_file_name = os.path.join("data", "data-processing-output", f"Abstimmungen_Details_{abst_date}.csv")
    print(f"Exporting to {export_file_name}...")
    concatenated_df.to_csv(export_file_name, index=False)

    common.upload_ftp(export_file_name, remote_path="wahlen_abstimmungen/abstimmungen")
    print("Job successful!")


def calculate_details(data_file_names):
    abst_date = ""
    appended_data = []
    print(f"Starting to work with data file(s) {data_file_names}...")
    columns_to_keep = [
        "Wahllok_name",
        "Stimmr_Anz",
        "Eingel_Anz",
        "Leer_Anz",
        "Unguelt_Anz",
        "Guelt_Anz",
        "Ja_Anz",
        "Nein_Anz",
        "Abst_Titel",
        "Abst_Art",
        "Abst_Datum",
        "Result_Art",
        "Abst_ID",
        "anteil_ja_stimmen",
        "abst_typ",
    ]
    for data_file_name in data_file_names:
        import_file_name = os.path.join("data", data_file_name)
        print(f"Reading dataset from {import_file_name} to retrieve sheet names...")
        sheets = pd.read_excel(import_file_name, sheet_name=None, skiprows=4, index_col=None)
        dat_sheet_names = []
        print('Determining "DAT n" sheets...')
        for key in sheets:
            if key.startswith("DAT "):
                dat_sheet_names.append(key)

        valid_wahllokale = [
            "Bahnhof SBB",
            "Rathaus",
            "Polizeiwache Clara",
            "Basel brieflich Stimmende",
            "Riehen Gemeindehaus",
            "Riehen brieflich Stimmende",
            "Bettingen Gemeindehaus",
            "Bettingen brieflich Stimmende",
            "Persönlich an der Urne Stimmende AS",
            "Brieflich Stimmende AS",
        ]

        # from 2023-06-18 onwards "Basel brieflich Stimmende" becomes "Basel briefl. & elektr. Stimmende (Total)"
        valid_wahllokale_ab_20230618 = [
            "Bahnhof SBB",
            "Rathaus",
            "Polizeiwache Clara",
            "Basel briefl. & elektr. Stimmende (Total)",
            "Riehen Gemeindehaus",
            "Riehen briefl. & elektr. Stimmende (Total)",
            "Bettingen Gemeindehaus",
            "Bettingen briefl. & elektr. Stimmende (Total)",
            "Persönlich an der Urne Stimmende AS",
            "Brieflich Stimmende AS",
            "Elektronisch Stimmende AS",
        ]

        # from 2025-09-01 onwards Kleinbasel as separate Wahllokal
        valid_wahllokale_ab_20250901 = [
            "Bahnhof SBB",
            "Rathaus",
            "Kleinbasel",
            "Basel briefl. & elektr. Stimmende (Total)",
            "Riehen Gemeindehaus",
            "Riehen briefl. & elektr. Stimmende (Total)",
            "Bettingen Gemeindehaus",
            "Bettingen briefl. & elektr. Stimmende (Total)",
            "Persönlich an der Urne Stimmende AS",
            "Brieflich Stimmende AS",
            "Elektronisch Stimmende AS",
        ]

        dat_sheets = []
        for sheet_name in dat_sheet_names:
            is_gegenvorschlag = False  # Is this a sheet that contains a Gegenvorschlag?
            print(f"Reading Abstimmungstitel from {sheet_name}...")
            df_title = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=4, index_col=None)
            abst_title_raw = df_title.columns[1]
            # Get String that starts form ')' plus space + 1 characters to the right
            abst_title = abst_title_raw[abst_title_raw.find(")") + 2 :]
            if "Gegenvorschlag" in abst_title:
                is_gegenvorschlag = True

            print(f"Reading Abstimmungsart and Date from {sheet_name}...")
            df_meta = pd.read_excel(import_file_name, sheet_name=sheet_name, skiprows=2, index_col=None)
            title_string = df_meta.columns[1]
            abst_type = "kantonal" if title_string.startswith("Kantonal") else "national"
            abst_date_raw = title_string[title_string.find("vom ") + 4 :]
            abst_date = dateparser.parse(abst_date_raw).strftime("%Y-%m-%d")

            print(f"Reading data from {sheet_name}...")
            df = pd.read_excel(
                import_file_name, sheet_name=sheet_name, skiprows=6, index_col=None
            )  # , header=[0, 1, 2])
            df.reset_index(inplace=True)

            print("Filtering out Wahllokale...")
            if abst_date < "2023-06-18":
                valid_wahllokale = valid_wahllokale
            elif abst_date < "2025-09-01":
                valid_wahllokale = valid_wahllokale_ab_20230618
            else:
                valid_wahllokale = valid_wahllokale_ab_20250901
            print("Valid Wahllokale are:", valid_wahllokale)
            print(df["Wahllokale"].unique())
            df = df[df["Wahllokale"].isin(valid_wahllokale)]
            print(df["Wahllokale"].unique())

            print("Renaming columns...")
            df.rename(
                columns={
                    "Wahllokale": "Wahllok_name",
                    "Unnamed: 2": "Stimmr_Anz",
                    "eingelegte": "Eingel_Anz",
                    "leere": "Leer_Anz",
                    "ungültige": "Unguelt_Anz",
                    "Total gültige": "Guelt_Anz",
                    "Ja": "Ja_Anz",
                    "Nein": "Nein_Anz",
                },
                inplace=True,
            )

            print("Setting cell values retrieved earlier...")
            df["Abst_Titel"] = abst_title
            df["Abst_Art"] = abst_type
            df["Abst_Datum"] = abst_date
            df["Abst_ID"] = sheet_name[sheet_name.find("DAT ") + 4]
            df["abst_typ"] = "Abstimmung ohne Gegenvorschlag / Stichfrage"

            df.Guelt_Anz.replace(0, pd.NA, inplace=True)  # Prevent division by zero errors

            if is_gegenvorschlag:
                print("Adding Gegenvorschlag data...")
                result_type = df_meta.columns[15]
                df.abst_typ = "Initiative mit Gegenvorschlag und Stichfrage"
                df.rename(
                    columns={
                        "Ja.1": "Gege_Ja_Anz",
                        "Nein.1": "Gege_Nein_Anz",
                        "Initiative": "Sti_Initiative_Anz",
                        "Gegen-vorschlag": "Sti_Gegenvorschlag_Anz",
                        "ohne gültige Antwort": "Init_OGA_Anz",
                        "ohne gültige Antwort.1": "Gege_OGA_Anz",
                        "ohne gültige Antwort.2": "Sti_OGA_Anz",
                    },
                    inplace=True,
                )

                print("Calculating anteil_ja_stimmen for Gegenvorschlag case...")
                for column in [
                    df.Ja_Anz,
                    df.Nein_Anz,
                    df.Gege_Ja_Anz,
                    df.Gege_Nein_Anz,
                    df.Sti_Initiative_Anz,
                    df.Sti_Gegenvorschlag_Anz,
                ]:
                    column.replace(0, pd.NA, inplace=True)  # Prevent division by zero errors

                df["anteil_ja_stimmen"] = df.Ja_Anz / (df.Ja_Anz + df.Nein_Anz)
                df["gege_anteil_ja_Stimmen"] = df.Gege_Ja_Anz / (df.Gege_Ja_Anz + df.Gege_Nein_Anz)
                df["sti_anteil_init_stimmen"] = df.Sti_Initiative_Anz / (
                    df.Sti_Initiative_Anz + df.Sti_Gegenvorschlag_Anz
                )
                columns_to_keep = columns_to_keep + [
                    "Gege_Ja_Anz",
                    "Gege_Nein_Anz",
                    "Sti_Initiative_Anz",
                    "Sti_Gegenvorschlag_Anz",
                    "gege_anteil_ja_Stimmen",
                    "sti_anteil_init_stimmen",
                    "Init_OGA_Anz",
                    "Gege_OGA_Anz",
                    "Sti_OGA_Anz",
                ]
            else:
                print("Adding data for case that is not with Gegenvorschlag...")
                result_type = df_meta.columns[8]
                print("Calculating anteil_ja_stimmen for case that is not with Gegenvorschlag...")
                df["anteil_ja_stimmen"] = df["Ja_Anz"] / df["Guelt_Anz"]

            df["Result_Art"] = result_type
            dat_sheets.append(df)

        print("Creating one dataframe for all Abstimmungen...")
        all_df = pd.concat(dat_sheets)
        print("Keeping only necessary columns...")
        all_df = all_df.filter(columns_to_keep)

        appended_data.append(all_df)
    print(f"Concatenating data from all import files ({appended_data})...")
    concatenated_df = pd.concat(appended_data)
    print("Calculating Abstimmungs-ID based on all data...")
    nat_df = concatenated_df[concatenated_df["Abst_Art"] == "national"]
    if "national" in nat_df["Abst_Art"].unique():
        max_nat_id = int(nat_df["Abst_ID"].max())
        concatenated_df["Abst_ID"] = np.where(
            concatenated_df["Abst_Art"] == "kantonal",
            max_nat_id + concatenated_df["Abst_ID"].astype("int32"),
            concatenated_df["Abst_ID"],
        )
    print('Creating column "Abst_ID_Titel"...')
    concatenated_df["Abst_ID_Titel"] = concatenated_df["Abst_ID"].astype(str) + ": " + concatenated_df["Abst_Titel"]
    # add Wahllokal_ID
    path_wahllokale = "data/Wahllokale.csv"
    df_wahllokale = pd.read_csv(path_wahllokale, encoding="unicode_escape")
    df_wahllokale.rename(columns={"Wahllok_Name": "Wahllok_name"}, inplace=True)
    concatenated_df = pd.merge(concatenated_df, df_wahllokale, on=["Wahllok_name"], how="inner")
    concatenated_df["id"] = (
        concatenated_df["Abst_Datum"]
        + "_"
        + concatenated_df["Abst_ID"].astype(str).str.zfill(2)
        + "_"
        + concatenated_df["wahllok_id"].astype(str)
    )
    return abst_date, concatenated_df


if __name__ == "__main__":
    print(f"Executing {__file__}...")
    main()
