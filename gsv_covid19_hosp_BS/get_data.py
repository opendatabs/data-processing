import pandas as pd
import requests
from gsv_covid19_hosp_BS import credentials
import datetime
import logging


def check_day(date=datetime.datetime.today()):
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


def filter_date(date):
    datefilter = "(CapacStamp gt datetime'" + str(date) + "T00:00:00'" + "or CapacStamp lt datetime'" + str(
        date) + "T23:59:59')"
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
    if hospital == 'UKBB':
        url2 = credentials.url_hosp_children + get_filter(hospital, date)
    else:
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
        df = df[["NoauResid", "CapacDate", "CapacTime", 'TotalAllBeds', 'TotalAllBedsC19', 'OperIcuBeds',
                 'OperIcuBedsC19', 'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats',
                 'TotalAllPatsC19', 'TotalIcuPats', 'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats',
                 'TotalImcPatsC19', 'EcmoPats']]
        df["Hospital"] = hospital
    return df
