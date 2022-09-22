import base64
import pandas as pd
import requests
from gsv_covid19_hosp_bl import credentials
from datetime import timezone, datetime
import logging


def create_auth_string(username, password):
    # Basic base64(username:password)
    message = f'{username}:{password}'
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    print(f'Basic {base64_message}')


def check_day(date=datetime.today()):
    logging.info("Check which day it is")
    if date.weekday() == 0:
        logging.info("It is Monday")
        return "Monday"
    elif date.weekday() in [1, 2, 3, 4]:
        logging.info("It's a workday other than Monday")
        return "Other workday"
    elif date.weekday() in [5, 6]:
        logging.info("It is weekend")
        return "Weekend"


def filter_hospital(hospital):
    dict_hospital = credentials.dict_hosp
    id_hospital = dict_hospital[hospital]
    hosp_filter = "(NoauResid eq " + id_hospital + ")"
    return hosp_filter


def convert_to_utc(date):
    string_start = str(date) + " 00:00:00"
    string_end = str(date) + " 23:59:59"
    naive_datetime_start = datetime.strptime(string_start, "%Y-%m-%d %H:%M:%S")
    datetime_utc_start = naive_datetime_start.astimezone(timezone.utc)
    string_utc_start = datetime_utc_start.strftime("%Y-%m-%dT%H:%M:%S")
    naive_datetime_end = datetime.strptime(string_end, "%Y-%m-%d %H:%M:%S")
    datetime_utc_end = naive_datetime_end.astimezone(timezone.utc)
    string_utc_end = datetime_utc_end.strftime("%Y-%m-%dT%H:%M:%S")
    return string_utc_start, string_utc_end


def filter_date(date):
    string_utc_start, string_utc_end = convert_to_utc(date)
    datefilter = f"(CapacStamp gt datetime'{string_utc_start}' or CapacStamp lt datetime'{string_utc_end}')"
    return datefilter


def get_filter(hospital, date):
    return "&$filter=(" + filter_date(date) + " and " + filter_hospital(hospital) + ")"


def get_data(hospital, date):
    logging.info(f"get entries out of IES for {hospital} on {date}")
    url = credentials.url_meta
    payload = {}
    headers = {
        'Authorization': credentials.authorization_live}
    requests.request("GET", url, headers=headers, data=payload)
    url2 = credentials.url_hosp_adults + get_filter(hospital, date)
    response = requests.request("GET", url2, headers=headers, data=payload)
    response.raise_for_status()
    results = response.json()["d"]["results"]
    return results


def get_dataframe(hospital, date):
    results = get_data(hospital, date)
    logging.info(f"Put IES entries into dataframe and filter out properties we need")
    df = pd.DataFrame(results)
    if not df.empty:
        df = df[["NoauResid", "CapacDate", "CapacTime", 'TotalAllBeds', 'OperIcuBeds',
                    'VentIcuBeds', 'OperImcBeds', 'TotalAllPats', 'TotalAllPatsC19',
                 'TotalIcuPats', 'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats',
                 'TotalImcPatsC19', 'VentImcPatsC19']]
        df["Hospital"] = hospital
    return df

if __name__ == "__main__":
    pass
