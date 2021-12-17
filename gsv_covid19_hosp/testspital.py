import datetime
from gsv_covid19_hosp import get_data
import mechanicalsoup
from bs4 import BeautifulSoup
from gsv_covid19_hosp import credentials
import logging
import common
import pandas as pd
import threading
from gsv_covid19_hosp import hospitalzahlen
from gsv_covid19_hosp import calculation
from gsv_covid19_hosp import update_coreport
import numpy as np
from math import nan

def run_test(hospital, date):
    if get_data.check_day() == "Monday":
        list_hospitals = [hospital]
        saturday = date - datetime.timedelta(2)
        df_saturday, missing_saturday = hospitalzahlen.get_df_for_date(date=saturday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals_sat = [x for x in list_hospitals if x not in missing_saturday]
        write_in_coreport_test(df_saturday, list_hospitals_sat, date=saturday)
        print("Missing Saturday: ", missing_saturday)
        sunday = date - datetime.timedelta(1)
        df_sunday, missing_sunday = hospitalzahlen.get_df_for_date(date=sunday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals_sun = [x for x in list_hospitals if x not in missing_sunday]
        write_in_coreport_test(df_sunday, list_hospitals_sun, date=sunday)
        print("Missing Sunday: ", missing_sunday)
        df_monday, missing_hospitals = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
        write_in_coreport_test(df_monday, filled_hospitals, date=date)
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(900, function=retry_test, args=[date, missing_hospitals]).start()
    elif get_data.check_day() == "Other workday":
        list_hospitals = [hospital]
        df, missing_hospitals = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if df.empty == False:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            write_in_coreport_test(df, filled_hospitals, date=date)
            print("entries in CoReport for ", filled_hospitals)
        elif df.empty == True:
            print("dataframe is empty, nothing is entered into CoReport")
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(900, function=retry_test, args=[date, missing_hospitals]).start()
    else:
        print("It is weekend")

def retry_test(date, list_hospitals):
    print("retrying")
    df, still_missing_hospitals = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
    if df.empty == False:
        write_in_coreport_test(df, hospital_list=list_hospitals, date=date)
        filled_hospitals = [x for x in list_hospitals if x not in still_missing_hospitals]
        print("entries in CoReport for ", filled_hospitals)
    if still_missing_hospitals is not []:
        print("Still missing: ", still_missing_hospitals)


def write_in_coreport_test(df, hospital, date):
    df_coreport = calculation.calculate_numbers(df)
    # df_coreport =coreport_scraper.add_value_id(df_coreport, date=date)

    # with value id's already saved the day before:
    date = date.strftime('%d.%m.%Y')
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df_value_id = pd.read_pickle(file_name)
    print(df_value_id)
    df_value_id["Hospital"] = hospital
    df_value_id.set_index("Hospital", inplace=True)
    df_coreport.set_index("Hospital", inplace=True)
    df_coreport = df_coreport.join(df_value_id)
    print(df_coreport)
    # no need to filter since just one hospital in test case:
    # df_hospital = df_coreport.filter(items=[hospital], axis=0)
    df_hospital = df_coreport
    properties = update_coreport.get_properties_list(hospital=hospital[0])
    print(properties)
    for prop in properties:
        # value_id = credentials.dict_coreport[hospital][prop]
        value = int(df_hospital[prop][0])
        value_id = df_hospital[prop + " value_id"][0]
        print(value_id, value)
        main_test(value_id=value_id, value=value)


def main_test(value_id, value, comment="Entered by bot"):

    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    payload = {
        "value": value,
        "comment": comment
}

    username = credentials.username_coreport_test
    password = credentials.password_coreport_test

    url = credentials.url_coreport + str(value_id)
    print(url)

    r = common.requests_patch(url, json=payload,
                              auth=(username, password))
    r.raise_for_status()


def make_df_value_id(date):
    username = credentials.username_coreport_test
    password = credentials.password_coreport_test

    browser = mechanicalsoup.StatefulBrowser()
    browser.open(credentials.url_login_coreport)
    browser.select_form()
    browser.form.print_summary()

    browser["login"] = username
    browser["password"] = password
    browser.submit_selected()

    date = date
    date = date.strftime('%d.%m.%Y')
    data_time = date + " 10:00"

    properties_list = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                       'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                       'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                       'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                       'Bettenanzahl belegt "IPS mit Beatmung"', 'Bettenanzahl frei " IPS ECMO"',
                       'Bettenanzahl belegt "IPS ECMO"']
    df = pd.DataFrame()

    for data_name in properties_list:
        df[data_name + " value_id"] = ""

    response = browser.get(credentials.url_coreport_test)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    for data_name in properties_list:
        df[data_name + " value_id"] = ""
        if data_name == 'Bettenanzahl frei "Normalstation"' and data_time == '14.12.2021 10:00':
            value_id = 442320
        else:
            tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
            value_id = tag["id"].replace('form-', '')
        print(value_id)
        df.loc[0, data_name + " value_id"] = value_id
    print(df)
    browser.close()
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df.to_pickle(file_name)


if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    datum = datetime.datetime.today().date() - datetime.timedelta(2)
    run_test('Clara', datum)
    # make_df_value_id(date=datum)
    # df = pd.read_pickle('value_id_df_test_15.12.2021.pkl')
    # pd.set_option('display.max_columns', None)
    # print(df)
    # print(df['Bettenanzahl frei " IPS ECMO" value_id'][0])
