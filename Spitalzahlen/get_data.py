import pandas as pd
import requests
import credentials
import json
import datetime

print(datetime.datetime.today().date())


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
        start_date = datetime.date.today() - datetime.timedelta(2)
        datefilter = "CapacStamp gt datetime'" + str(start_date) + "T00:00:00'" + "or CapacStamp lt datetime'" + str(
            date) + "T23:59:59'"
        return datefilter
    elif check_day() == "Other workday":
        datefilter = "(CapacStamp gt datetime'" + str(date) + "T00:00:00'" + "or CapacStamp lt datetime'" + str(
            date) + "T23:59:59')"
        return datefilter


def filter_hospitals(hospital):

    return "(NoauResid eq '00000000000000047212')"


def filter():
    return "&$filter=(" + filter_dates() + " and " + filter_hospitals() + ")"


def get_data():
    url = "https://www.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/$metadata"
    payload = {}
    headers = {
        'Authorization': credentials.authorization_live}
    requests.request("GET", url, headers=headers, data=payload)
    url2 = "https://www.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapAdultSet?$format=json" + filter()
    print(url2)
    response = requests.request("GET", url2, headers=headers, data=payload)
    results = response.json()["d"]["results"]
    return results

def make_dataframe():
    results = get_data()
    df = pd.DataFrame(results)
    df = df[ ["NoauResid", "CapacDate", "CapacTime", 'TotalAllBeds', 'TotalAllBedsC19', 'OperIcuBeds', 'OperIcuBedsC19',
     'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19', 'TotalIcuPats',
     'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats', 'TotalImcPatsC19', 'EcmoPats']]
    return df




