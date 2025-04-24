import json
import logging
import os
import pathlib
from datetime import datetime

import common
import geopandas as gpd
import pandas as pd
from Crypto.Hash import (
    HMAC,  # use package pycryptodome
    SHA256,
)
from dotenv import load_dotenv
from requests.auth import AuthBase

load_dotenv()

PUBLIC_KEY = os.getenv("PUBLIC_KEY_FIELDCLIMATE")
PRIVATE_KEY = os.getenv("PRIVATE_KEY_FIELDCLIMATE")
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_08")
FTP_PASS = os.getenv("FTP_PASS_08")


# Class to perform HMAC encoding
class AuthHmacMetosGet(AuthBase):
    # Creates HMAC authorization header for Metos REST service GET request.
    def __init__(self, api_route, public_key, private_key):
        self._publicKey = public_key
        self._privateKey = private_key
        self._method = "GET"
        self._apiRoute = api_route

    def __call__(self, request):
        date_stamp = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        logging.info(f"timestamp:  {date_stamp}")
        request.headers["Date"] = date_stamp
        msg = (self._method + self._apiRoute + date_stamp + self._publicKey).encode(
            encoding="utf-8"
        )
        h = HMAC.new(self._privateKey.encode(encoding="utf-8"), msg, SHA256)
        signature = h.hexdigest()
        request.headers["Authorization"] = "hmac " + self._publicKey + ":" + signature
        return request


def call_fieldclimate_api(api_uri, api_route, filename):
    auth = AuthHmacMetosGet(api_route, PUBLIC_KEY, PRIVATE_KEY)
    response = common.requests_get(
        url=api_uri + api_route, headers={"Accept": "application/json"}, auth=auth
    )
    parsed = json.loads(response.text)
    # logging.info(response.json())
    pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
    # logging.info(pretty_resp)
    with open(os.path.join("data", f"json/{filename}.json"), "w") as f:
        f.write(pretty_resp)

    normalized = pd.json_normalize(parsed)
    return pretty_resp, normalized


def main():
    logging.info(
        "Retrieving information about all stations of current user from API..."
    )
    (pretty_resp, df) = call_fieldclimate_api(
        "https://api.fieldclimate.com/v2",
        "/user/stations",
        f"stations-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}",
    )
    logging.info(
        "Filtering stations with altitude not set to null, only those are live..."
    )
    # mast_frame = stations_frame[stations_frame['name.custom'].str.contains('Mast')
    #                             & ~stations_frame['name.custom'].str.contains('A2')]
    live_df = df.loc[pd.notnull(df["position.altitude"])]
    now = datetime.now()
    folder = now.strftime("%Y-%m")
    local_folder = f"data/csv/val/{folder}"
    pathlib.Path(local_folder).mkdir(parents=True, exist_ok=True)
    filename_val = f"{local_folder}/stations--{now.strftime('%Y-%m-%dT%H-%M-%S%z')}.csv"
    logging.info("Ensuring columns exist...")
    column_names = [
        "name.original",
        "name.custom",
        "dates.min_date",
        "dates.max_date",
        "config.timezone_offset",
        "meta.time",
        "meta.rh",
        "meta.airTemp",
        "meta.rain24h.vals",
        "meta.rain24h.sum",
        "meta.rain48h.sum",
    ]
    for column_name in column_names:
        if column_name not in live_df.columns:
            live_df[column_name] = None
    logging.info(f"Saving live stations to {filename_val}...")
    live_val = live_df[column_names]
    logging.info("Getting last hour's precipitation...")
    pd.options.mode.chained_assignment = (
        None  # Switch off warnings, see https://stackoverflow.com/a/53954986
    )
    # make sure we have a list present, otherwise return None, see https://stackoverflow.com/a/12709152/5005585
    live_val["meta.rain.1h.val"] = live_df["meta.rain24h.vals"].apply(
        lambda x: x[23] if isinstance(x, list) else None
    )
    live_val.to_csv(filename_val, index=False)
    map_df = live_df[
        [
            "name.original",
            "name.custom",
            "dates.min_date",
            "dates.max_date",
            "position.altitude",
            "config.timezone_offset",
            "position.geo.coordinates",
        ]
    ]
    logging.info(
        "Stations with name.custom of length 1 are not live yet, filter those out..."
    )
    # For some reason we have to filter > 2 here
    # map_df['name.custom.len'] = map_df['name.custom'].str.len()
    live_map = map_df.loc[map_df["name.custom"].str.len() > 2]
    logging.info("Reversing coordinates for ods...")
    # Cast 'position.geo.coordinates' to a list if possible
    live_map["Lon"] = live_map["position.geo.coordinates"].apply(lambda x: x[0])
    live_map["Lat"] = live_map["position.geo.coordinates"].apply(lambda x: x[1])
    live_map["coords"] = live_map["position.geo.coordinates"].apply(
        lambda x: f"{x[1]},{x[0]}"
    )
    # Calculate distance to 47.557, 7.593 (Münsterfähre) from coords with help of Geopandas
    live_map["distance"] = live_map["coords"].apply(
        lambda x: gpd.points_from_xy(
            [float(x.split(",")[1])], [float(x.split(",")[0])]
        )[0].distance(gpd.points_from_xy([7.593], [47.557])[0])
    )
    # Replace every value in coords with None if distance is greater than 1
    live_map["coords"] = live_map["coords"].where(live_map["distance"] < 1, None)
    live_map = live_map.drop(columns=["distance"])
    filename_stations_map = os.path.join("data", "csv", "map", "stations.csv")
    logging.info(
        f"Saving minimized table of station data for map creation to {filename_stations_map}"
    )
    live_map.to_csv(filename_stations_map, index=False)
    # logging.info("Retrieving last hour's data from all live stations from API...")
    # for station in df['name.original']:
    #     # get last data point from each station. See https://api.fieldclimate.com/v1/docs/#info-understanding-your-device
    #     (pretty_resp, station_df) = call_fieldclimate_api('/data/normal/' + station + '/hourly/last/1h',
    #                                                       publicKey, privateKey, f'station--{station}--{datetime.now()}')
    common.upload_ftp(filename_stations_map, FTP_SERVER, FTP_USER, FTP_PASS, "map")
    common.ensure_ftp_dir(FTP_SERVER, FTP_USER, FTP_PASS, f"val/{folder}")
    common.upload_ftp(filename_val, FTP_SERVER, FTP_USER, FTP_PASS, f"val/{folder}")

    # Iterate over all json files in the json folder and upload them to the FTP server deleting them afterward
    for json_file in os.listdir(os.path.join("data", "json")):
        if not json_file.endswith(".json"):
            continue
        jahr = json_file.split("-")[1]
        common.upload_ftp(
            os.path.join("data", "json", json_file),
            FTP_SERVER,
            FTP_USER,
            FTP_PASS,
            f"json/{jahr}",
        )
        os.remove(os.path.join("data", "json", json_file))
    logging.info("Job successful!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
