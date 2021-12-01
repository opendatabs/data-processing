import pandas as pd
import requests
import credentials
# import json
import datetime


def check_day():
    if datetime.datetime.today().weekday() == 0:
        return "Monday"
    elif datetime.datetime.today().weekday() in [1, 2, 3, 4]:
        return "Other workday"
    elif datetime.datetime.today().weekday() in [5, 6]:
        # print warning?
        return "Weekend"


def filter_dates():
    date = datetime.datetime.today().date()
    if check_day() == "Monday":
        start_date = datetime.date.today().date() - datetime.timedelta(2)
        datefilter = "CapacStamp gt datetime'" + str(start_date) + "T00:00:00'" + "or CapacStamp lt datetime'" + str(
            date) + "T23:59:59'"
        return datefilter
    elif check_day() == "Other workday":
        datefilter = "(CapacStamp gt datetime'" + str(date) + "T00:00:00'" + "or CapacStamp lt datetime'" + str(
            date) + "T23:59:59')"
        return datefilter


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
    results = response.json()["d"]["results"]
    return results


def get_dataframe(hospital, date):
    results = get_data(hospital, date)
    df = pd.DataFrame(results)
    if df.empty == False:
        if hospital == 'UKBB':
            df = df[["NoauResid", "CapacDateStr", "CapacTimeStr", 'TotalAllBeds', 'TotalAllBedsC19', 'OperIcuBeds',
                 'OperIcuBedsC19', 'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19',
                 'TotalIcuPats', 'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats', 'TotalImcPatsC19', 'EcmoPats']]
            df = df.rename(columns={"CapacDateStr": "CapacDate", "CapacTimeStr": "CapacTime"})
        else:
            df = df[
                ["NoauResid", "CapacDate", "CapacTime", 'TotalAllBeds', 'TotalAllBedsC19', 'OperIcuBeds', 'OperIcuBedsC19',
                'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19', 'TotalIcuPats',
                'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats', 'TotalImcPatsC19', 'EcmoPats']]
    return df


def make_dataframe(list_hospitals):
    df = pd.DataFrame()
    for hospital in list_hospitals:
        df_hospital = get_dataframe(hospital)
        df = pd.concat([df, df_hospital])
    return df
