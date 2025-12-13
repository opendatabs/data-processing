import io
import json
import logging
import os

import common
import pandas as pd
from common import change_tracking as ct
from dotenv import load_dotenv

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_09")
FTP_PASS = os.getenv("FTP_PASS_09")


def create_files_for_dashboard(df, filename):
    """
    Creates JSON files for the dashboard based on the provided DataFrame.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - filename (str): The name of the file to process.
    """
    # Define categories based on filename
    categories = {
        "MIV_Speed.csv": [
            "Total",
            "<20",
            "20-30",
            "30-40",
            "40-50",
            "50-60",
            "60-70",
            "70-80",
            "80-90",
            "90-100",
            "100-110",
            "110-120",
            "120-130",
            ">130",
        ],
        "MIV_Class_10_1.csv": [
            "Total",
            "MR",
            "PW",
            "PW+",
            "Lief",
            "Lief+",
            "Lief+Aufl.",
            "LW",
            "LW+",
            "Sattelzug",
            "Bus",
            "andere",
        ],
        "FLIR_KtBS_MIV6.csv": ["Total", "MR", "PW", "Lief", "LW", "Sattelzug", "Bus"],
        "Velo_Fuss_Count.csv": ["Total"],
        "LSA_Count.csv": ["Total"],
        "FLIR_KtBS_Velo.csv": ["Total"],
        "FLIR_KtBS_FG.csv": ["Total"],
    }

    # Create a separate dataset per site and traffic type
    all_sites = df.Zst_id.unique()
    for site in all_sites:
        for traffic_type in ["MIV", "Velo", "Fussgänger"]:
            site_data = df[df.Zst_id.eq(site) & df.TrafficType.eq(traffic_type)]

            if site_data.empty:
                continue

            # Determine subfolder based on traffic type and filename
            if traffic_type == "Fussgänger":
                subfolder = "Fussgaenger"
            elif filename == "MIV_Speed.csv":
                subfolder = "MIV_Speed"
            else:
                subfolder = traffic_type

            # Save the original site data
            current_filename = os.path.join("data", "sites", subfolder, f"{str(site)}.csv")
            logging.info(f"Saving {current_filename}...")
            site_data.to_csv(current_filename, sep=";", encoding="utf-8", index=False)

            if ct.has_changed(current_filename):
                # Add Direction_LaneName column
                site_data["Direction_LaneName"] = (
                    site_data["DirectionName"].astype(str) + "#" + site_data["LaneName"].astype(str)
                )
                # Convert Date to string format like '2022-01-01'
                site_data["Date"] = pd.to_datetime(site_data["Date"], format="%d.%m.%Y").dt.strftime("%Y-%m-%d")

                # Perform aggregations
                aggregate_hourly(site_data, categories, subfolder, site, filename)
                aggregate_daily(site_data, categories, subfolder, site, filename)
                aggregate_monthly(site_data, categories, subfolder, site, filename)
                aggregate_yearly(site_data, categories, subfolder, site, filename)

                ct.update_hash_file(current_filename)

    # Calculate DTV per ZST and traffic type
    df_locations = download_locations()
    unique_traffic_types = df.TrafficType.unique()
    if "MIV" in unique_traffic_types:
        df_dtv = calculate_dtv_zst_miv(df, df_locations, filename)
        if "MIV_Speed" in filename:
            current_filename = os.path.join("data", "dtv_MIV_Speed.json")
        else:
            current_filename = os.path.join("data", "dtv_MIV.json")
        logging.info(f"Saving {current_filename}...")
        save_as_list_of_lists(df_dtv, current_filename)
    else:
        df_dtv_velo, df_dtv_fuss = calculate_dtv_zst_velo_fuss(df, df_locations)
        if not df_dtv_velo.empty:
            current_filename_velo = os.path.join("data", "dtv_Velo.json")
            logging.info(f"Saving {current_filename_velo}...")
            save_as_list_of_lists(df_dtv_velo, current_filename_velo)
        if not df_dtv_fuss.empty:
            current_filename_fuss = os.path.join("data", "dtv_Fussgaenger.json")
            logging.info(f"Saving {current_filename_fuss}...")
            save_as_list_of_lists(df_dtv_fuss, current_filename_fuss)


def upload_list_of_lists():
    filenames = [
        "dtv_MIV.json",
        "dtv_MIV_Speed.json",
        "dtv_Velo.json",
        "dtv_Fussgaenger.json",
    ]
    for filename in filenames:
        current_path = os.path.join("data", filename)
        if os.path.exists(current_path):
            common.upload_ftp(
                current_path,
                FTP_SERVER,
                FTP_USER,
                FTP_PASS,
                "verkehrszaehl_dashboard/data",
            )
            os.remove(current_path)


def save_upload_remove(df, filepath, ftp_subfolder):
    """
    Saves a DataFrame to a CSV file, uploads it via FTP, and removes the file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - filepath (str): The local file path to save the CSV.
    - ftp_subfolder (str): The FTP subfolder to upload the file to.
    """
    logging.info(f"Saving {filepath}...")
    df.to_csv(filepath, sep=";", encoding="utf-8", index=False)
    common.upload_ftp(filepath, FTP_SERVER, FTP_USER, FTP_PASS, ftp_subfolder)
    os.remove(filepath)


def aggregate_hourly(site_data, categories, subfolder, site, filename):
    """
    Performs hourly aggregation and saves the data.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """

    # Determine the date range
    min_date = pd.to_datetime(site_data["Date"]).min()
    max_date = pd.to_datetime(site_data["Date"]).max()
    date_range = pd.DataFrame({"Date": pd.date_range(start=min_date, end=max_date).strftime("%Y-%m-%d")})

    for category in categories[filename]:
        # Calculate the total counts per hour for each date, direction, and lane
        df_to_pivot = site_data[["Date", "Direction_LaneName", "HourFrom", category]].copy()
        df_agg = df_to_pivot.pivot_table(
            index=["Date", "Direction_LaneName"],
            values=category,
            columns="HourFrom",
            aggfunc="sum",
        ).reset_index()
        # Create the complete date range for each direction and lane combination
        directions_lanes = df_agg["Direction_LaneName"].unique()
        complete_dates = pd.MultiIndex.from_product(
            [date_range["Date"], directions_lanes], names=["Date", "Direction_LaneName"]
        ).to_frame(index=False)
        df_agg = complete_dates.merge(df_agg, on=["Date", "Direction_LaneName"], how="left")

        df_agg["Weekday"] = pd.to_datetime(df_agg["Date"]).dt.weekday
        df_agg[["DirectionName", "LaneName"]] = df_agg["Direction_LaneName"].str.split("#", expand=True)
        df_agg = df_agg.drop(columns=["Direction_LaneName"])

        # Save the hourly data
        save_upload_remove(
            df_agg,
            os.path.join("data", "sites", subfolder, f"{str(site)}_{category}_hourly.csv"),
            f"verkehrszaehl_dashboard/data/{subfolder}",
        )


def aggregate_daily(site_data, categories, subfolder, site, filename):
    """
    Performs daily aggregation and saves the data.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """
    # Calculate the daily counts per weekday for each date, direction, and lane
    df_to_group = site_data[["Date", "Direction_LaneName"] + categories[filename] + ["ValuesApproved", "ValuesEdited"]].copy()

    # Determine the date range
    min_date = pd.to_datetime(site_data["Date"]).min()
    max_date = pd.to_datetime(site_data["Date"]).max()
    date_range = pd.DataFrame({"Date": pd.date_range(start=min_date, end=max_date).strftime("%Y-%m-%d")})

    df_agg = df_to_group.groupby(["Date", "Direction_LaneName"])[categories[filename] + ["ValuesApproved", "ValuesEdited"]].sum().reset_index()
    df_agg = df_agg[df_agg["Total"] > 0]
    df_agg["Weekday"] = pd.to_datetime(df_agg["Date"]).dt.weekday
    df_agg["Week"] = pd.to_datetime(df_agg["Date"]).dt.isocalendar().week
    df_agg["Year"] = pd.to_datetime(df_agg["Date"]).dt.year

    # Create the complete date range for each direction and lane combination
    directions_lanes = df_agg["Direction_LaneName"].unique()
    complete_dates = pd.MultiIndex.from_product(
        [date_range["Date"], directions_lanes], names=["Date", "Direction_LaneName"]
    ).to_frame(index=False)
    df_agg = complete_dates.merge(df_agg, on=["Date", "Direction_LaneName"], how="left")

    df_agg[["DirectionName", "LaneName"]] = df_agg["Direction_LaneName"].str.split("#", expand=True)
    df_agg = df_agg.drop(columns=["Direction_LaneName"])

    # Save the daily data
    save_upload_remove(
        df_agg,
        os.path.join("data", "sites", subfolder, f"{str(site)}_daily.csv"),
        f"verkehrszaehl_dashboard/data/{subfolder}",
    )


def aggregate_monthly(site_data, categories, subfolder, site, filename):
    """
    Aggregates data over months.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """
    group_cols = ["Year", "Month", "Direction_LaneName"]
    df_to_group = site_data[group_cols + ["DateTimeFrom"] + categories[filename] + ["ValuesApproved", "ValuesEdited"]].copy()

    # Aggregate data by month
    df_agg = df_to_group.groupby(group_cols)[categories[filename] + ["ValuesApproved", "ValuesEdited"]].sum().reset_index()
    df_agg = df_agg[df_agg["Total"] > 0]

    # Count the number of measures
    df_measures = df_to_group.groupby(group_cols)["DateTimeFrom"].nunique().reset_index()
    df_measures.rename(columns={"DateTimeFrom": "NumMeasures"}, inplace=True)
    df_agg = df_agg.merge(df_measures, on=group_cols, how="left")
    for col in categories[filename]:
        df_agg[col] = df_agg[col] / df_agg["NumMeasures"] * 24

    df_agg[["DirectionName", "LaneName"]] = df_agg["Direction_LaneName"].str.split("#", expand=True)
    df_agg = df_agg.drop(columns=["Direction_LaneName"])

    # Save the aggregated data
    save_upload_remove(
        df_agg,
        os.path.join("data", "sites", subfolder, f"{str(site)}_monthly.csv"),
        f"verkehrszaehl_dashboard/data/{subfolder}",
    )


def aggregate_yearly(site_data, categories, subfolder, site, filename):
    """
    Aggregates data over years.

    Parameters:
    - site_data (pd.DataFrame): DataFrame containing site data.
    - categories (dict): Dictionary mapping filenames to category lists.
    - subfolder (str): Subfolder name.
    - site (str/int): Site identifier.
    - filename (str): Name of the file being processed.
    """
    group_cols = ["Year", "Direction_LaneName"]
    df_to_group = site_data[group_cols + ["DateTimeFrom"] + categories[filename] + ["ValuesApproved", "ValuesEdited"]].copy()

    # Aggregate data by year
    df_agg = df_to_group.groupby(group_cols)[categories[filename] + ["ValuesApproved", "ValuesEdited"]].sum().reset_index()
    df_agg = df_agg[df_agg["Total"] > 0]

    # Count the number of measures
    df_measures = df_to_group.groupby(group_cols)["DateTimeFrom"].nunique().reset_index()
    df_measures.rename(columns={"DateTimeFrom": "NumMeasures"}, inplace=True)
    df_agg = df_agg.merge(df_measures, on=group_cols, how="left")
    for col in categories[filename]:
        df_agg[col] = df_agg[col] / df_agg["NumMeasures"] * 24

    # Create a complete range of years
    min_year = site_data["DateTimeFrom"].min().year
    max_year = site_data["DateTimeFrom"].max().year
    years = pd.DataFrame({"Year": range(min_year, max_year + 1)})
    direction_lanes = site_data["Direction_LaneName"].unique()
    complete_years = pd.MultiIndex.from_product(
        [years["Year"], direction_lanes], names=["Year", "Direction_LaneName"]
    ).to_frame(index=False)

    # Merge with the complete range
    df_agg = complete_years.merge(df_agg, on=group_cols, how="left")
    df_agg[["DirectionName", "LaneName"]] = df_agg["Direction_LaneName"].str.split("#", expand=True)
    df_agg = df_agg.drop(columns=["Direction_LaneName"])

    # Save the aggregated data
    save_upload_remove(
        df_agg,
        os.path.join("data", "sites", subfolder, f"{str(site)}_yearly.csv"),
        f"verkehrszaehl_dashboard/data/{subfolder}",
    )


def download_locations():
    """
    Downloads location data and returns a DataFrame with the locations.

    Returns:
    - pd.DataFrame: DataFrame containing location data.
    """
    url_to_locations = "https://data.bs.ch/explore/dataset/100038/download/"
    params = {
        "format": "csv",
        "timezone": "Europe/Zurich",
        "klasse": "Dauerzaehlstelle",
    }
    r = common.requests_get(url_to_locations, params=params)
    df_locations = pd.read_csv(io.StringIO(r.text), sep=";", encoding="utf-8")
    # Expand 'zweck' to several lines if there is a '+'
    df_locations["zweck"] = df_locations["zweck"].str.split("+")
    df_locations = df_locations.explode("zweck")
    # Replace 'Velo/Moto' with 'Velo' and 'Fuss' with 'Fussgänger'
    df_locations["zweck"] = df_locations["zweck"].str.replace("Velo/Moto", "Velo")
    df_locations["zweck"] = df_locations["zweck"].str.replace("Fuss", "Fussgänger")
    # Save 'id_zst' as string
    df_locations["id_zst"] = df_locations["id_zst"].astype(str)
    return df_locations[
        [
            "id_zst",
            "zweck",
            "geo_point_2d",
            "name",
            "gemeinde",
            "klasse",
            "kombiniert",
            "art",
            "arme",
            "fahrstreif",
            "typ",
            "strtyp",
            "betriebnah",
            "betriebzus",
        ]
    ]


def merge_dtv_with_counts_and_locations(df, df_dtv, df_locations):
    """
    Merges DTV data with counts and location data.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - df_dtv (pd.DataFrame): The DataFrame containing DTV data.
    - df_locations (pd.DataFrame): The DataFrame containing location data.

    Returns:
    - pd.DataFrame: The merged DataFrame.
    """
    df_count = df.groupby(["Zst_id", "TrafficType"])["DateTimeFrom"].count().reset_index()
    df_count = df_count[df_count["DateTimeFrom"] > 0]
    df_dtv = df_dtv.merge(df_count, on=["Zst_id", "TrafficType"], how="left")
    df_dtv.rename(columns={"DateTimeFrom": "NumMeasures"}, inplace=True)
    df_dtv = df_dtv.merge(
        df_locations,
        left_on=["Zst_id", "TrafficType"],
        right_on=["id_zst", "zweck"],
        how="left",
    ).drop(columns=["id_zst", "zweck"])
    return df_dtv


def calculate_dtv_zst_miv(df, df_locations, filename):
    """
    Calculates DTV per ZST for MIV data.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - df_locations (pd.DataFrame): DataFrame containing location data.
    - filename (str): The name of the file to process.

    Returns:
    - pd.DataFrame: DataFrame containing DTV data.
    """
    aggregation_dict = {
        "MIV_Speed.csv": [
            "Total",
            "<20",
            "20-30",
            "30-40",
            "40-50",
            "50-60",
            "60-70",
            "70-80",
            "80-90",
            "90-100",
            "100-110",
            "110-120",
            "120-130",
            ">130",
        ],
        "MIV_Class_10_1.csv": [
            "Total",
            "MR",
            "PW",
            "PW+",
            "Lief",
            "Lief+",
            "Lief+Aufl.",
            "LW",
            "LW+",
            "Sattelzug",
            "Bus",
            "andere",
        ],
        "FLIR_KtBS_MIV6.csv": ["Total", "MR", "PW", "Lief", "LW", "Sattelzug", "Bus"],
        "LSA_Count.csv": ["Total"],
    }
    if filename in aggregation_dict:
        columns = aggregation_dict[filename]
        df_tv = df.groupby(["Zst_id", "Date", "TrafficType"])[columns].sum().reset_index()
        # Remove rows with Total = 0
        df_tv = df_tv[df_tv["Total"] > 0]
        df_dtv = df_tv.groupby(["Zst_id", "TrafficType"])[columns].mean().reset_index()

        df_dtv = merge_dtv_with_counts_and_locations(df, df_dtv, df_locations)
        # Drop Velo in the TrafficType column (just to be sure)
        df_dtv = df_dtv[df_dtv["TrafficType"] != "Velo"]
        return df_dtv
    return None


def calculate_dtv_zst_velo_fuss(df, df_locations):
    """
    Calculates DTV per ZST for Velo and Fussgänger data.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - df_locations (pd.DataFrame): DataFrame containing location data.
    Returns:
    - tuple: A tuple containing two DataFrames for Velo and Fussgänger data.
    """
    df_tv = df.groupby(["Zst_id", "Date", "TrafficType"])["Total"].sum().reset_index()
    # Remove rows with Total = 0
    df_tv = df_tv[df_tv["Total"] > 0]
    df_dtv = df_tv.groupby(["Zst_id", "TrafficType"])["Total"].mean().reset_index()
    # Remove rows with NaN values
    df_dtv = df_dtv.dropna()

    df_dtv = merge_dtv_with_counts_and_locations(df, df_dtv, df_locations)

    df_dtv_velo = df_dtv[df_dtv["TrafficType"] == "Velo"]
    df_dtv_fuss = df_dtv[df_dtv["TrafficType"] == "Fussgänger"]
    df_dtv_fuss["TrafficType"] = "Fussgaenger"  # Correcting the TrafficType for consistency
    return df_dtv_velo, df_dtv_fuss


def save_as_list_of_lists(df, filename):
    """
    Saves a DataFrame as a JSON file in a list-of-lists format.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - filename (str): The output JSON file path.
    """
    # Convert datetime columns to string format
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64", "datetimetz"]):
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Read the existing JSON file if it exists
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        # data[0] are the column names
        columns = data[0]
        # data[1:] are the data rows
        rows = data[1:]
        df_existing = pd.DataFrame(rows, columns=columns)
        df = pd.concat([df_existing, df], ignore_index=True)
        # Replace NaN values with -1
        df = df.fillna(-1)

    # Convert DataFrame to list of lists
    data_as_list = [df.columns.tolist()] + df.to_numpy().tolist()

    # Save as JSON
    with open(filename, "w", encoding="utf-8") as json_file:
        json.dump(data_as_list, json_file, ensure_ascii=False, indent=4)
    logging.info(f"Saved {filename} in list-of-lists format.")


def download_weather_station_data():
    """
    Downloads weather station data and returns a DataFrame with the data.

    Returns:
    - pd.DataFrame: DataFrame containing weather station data.
    """
    url_to_locations = "https://data.bs.ch/explore/dataset/100254/download/"
    params = {
        "format": "csv",
        "timezone": "Europe/Zurich",
    }
    r = common.requests_get(url_to_locations, params=params)
    df = pd.read_csv(io.StringIO(r.text), sep=";", encoding="utf-8")
    # Extract Date, HourFrom, Year, Month, Week, Weekday from datum_zeit
    df = df.rename(
        columns={
            "date": "Date",
            "tre200d0": "temp_c",
            "tre200dn": "temp_min",
            "tre200dx": "temp_max",
            "rre150d0": "prec_mm",
        }
    )
    df = df[df["Date"] > "2000-01-01"]
    df["Year"] = pd.to_datetime(df["Date"]).dt.year
    df["Month"] = pd.to_datetime(df["Date"]).dt.month
    df["Weekday"] = pd.to_datetime(df["Date"]).dt.weekday

    # Aggregate daily data for tre200d0, tre200dn, tre200dx and rre150d0 columns
    df = df[
        [
            "Date",
            "temp_c",
            "temp_min",
            "temp_max",
            "prec_mm",
            "Year",
            "Month",
            "Weekday",
        ]
    ].copy()
    # Sort by Date starting with the oldest
    df = df.sort_values(by="Date")

    # Save the daily data
    current_filename_daily = os.path.join("data", "weather", "weather_daily.csv")
    logging.info(f"Saving {current_filename_daily}...")
    df.to_csv(current_filename_daily, sep=";", encoding="utf-8", index=False)
    common.upload_ftp(
        current_filename_daily,
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        "verkehrszaehl_dashboard/data/weather",
    )
    os.remove(current_filename_daily)
    # Aggregate yearly data for tre200d0
    df_to_group = df[["Year", "temp_c"]].copy()
    df_agg = df_to_group.groupby(["Year"])[["temp_c"]].mean().reset_index()
    # Remove current year
    df_agg = df_agg[df_agg["Year"] != pd.Timestamp.now().year]
    # Save the yearly data
    current_filename_yearly = os.path.join("data", "weather", "weather_yearly.csv")
    logging.info(f"Saving {current_filename_yearly}...")
    df_agg.to_csv(current_filename_yearly, sep=";", encoding="utf-8", index=False)
    common.upload_ftp(
        current_filename_yearly,
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        "verkehrszaehl_dashboard/data/weather",
    )
    os.remove(current_filename_yearly)
