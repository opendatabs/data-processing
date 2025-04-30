import pandas as pd

from gsv_covid19_hosp_bs import credentials
from gsv_covid19_hosp_bs import hospitalzahlen, calculation
import common
import requests
import logging
from datetime import timezone, datetime, timedelta
from zoneinfo import ZoneInfo
pd.set_option("display.max_rows", 999)
pd.set_option("display.max_columns", 999)

def add_value_id(df, date):
    url_api = credentials.url_coreport_api
    username = credentials.username_coreport
    password = credentials.password_coreport
    timeslot = date.strftime('%d-%m-%Y')
    columns = list(df.columns[4:])
    dict_org = credentials.dict_organization
    for data_name in columns:
        df[data_name + " value_id"] = ""
    hospitals = list(df["Hospital"])
    df.set_index("Hospital", inplace=True)
    for hospital in hospitals:
        organization = dict_org[hospital]
        if hospital == 'USB':
            data_names = columns
        else:
            data_names = [ x for x in columns if x not in ['Bettenanzahl frei " IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"']]
        for data_name in data_names:
            filter = f'&organization={organization}&timeslot={timeslot}&question={data_name}'
            url = url_api + filter
            req = common.requests_get(url, auth=(username, password))
            result = req.json()[0]
            # make sure first result indeed has the right date
            assert result['timeslot']['deadline'] == timeslot
            value_id = result['id']
            df.loc[hospital, data_name + " value_id"] = value_id
    return df

if __name__ == "__main__":
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    date = now_in_switzerland.date()
    list_hospitals = ['Clara', 'UKBB', 'USB']
    df, missing = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
    logging.info("Calculate numbers for CoReport")
    df_coreport = calculation.calculate_numbers(df)
    logging.info("Get value id's from CoReport")
    df_coreport = add_value_id(df_coreport, date=date)
    print(df_coreport)