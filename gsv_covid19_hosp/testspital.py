from datetime import timezone, datetime, timedelta
from gsv_covid19_hosp import get_data
import mechanicalsoup
from bs4 import BeautifulSoup
from gsv_covid19_hosp import credentials
import logging
import common
import pandas as pd
from gsv_covid19_hosp import hospitalzahlen
from gsv_covid19_hosp import calculation
from gsv_covid19_hosp import update_coreport
from zoneinfo import ZoneInfo


now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
print(now_in_switzerland)

date = now_in_switzerland.date()

time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=15).astimezone(
    ZoneInfo('Europe/Zurich'))
print(time_for_email)


def run_test(hospital, date):
    list_hospitals = [hospital]
    day_of_week = get_data.check_day(date)
    df_log_file = make_log_file(date, day_of_week, list_hospitals)
    if day_of_week == "Monday":
        try_to_enter_in_coreport(date=date - timedelta(2), day="Saturday", list_hospitals=list_hospitals, weekend=True)
        try_to_enter_in_coreport(date=date - timedelta(1), day="Sunday", list_hospitals=list_hospitals, weekend=True)
        try_to_enter_in_coreport(date=date, day="today", list_hospitals=list_hospitals, weekend=False)
    elif day_of_week == "Other workday":
        try_to_enter_in_coreport(date=date, day="today", list_hospitals=list_hospitals, weekend=False)
    else:
        logging.info("It is weekend")
    print(df_log_file)
    df_log_file.to_csv("log_file.csv")


def make_log_file(date, day_of_week, list_hospitals):
    df = pd.DataFrame()
    l = len(list_hospitals)
    if day_of_week == "Monday":
        df["Date"] = [date - timedelta(2)] * l + [date - timedelta(1)] * l + [date] * l
        df["Hospital"] = list_hospitals * 3
    elif day_of_week == "Other workday":
        df["Date"] = [date] * l
        df["Hospital"] = list_hospitals * 3
    df["IES entry"] = 0
    df["CoReport filled"] = 0
    df["email"] = 0
    df["second email"] = 0
    return df


def write_in_coreport_test(df, hospital, date):
    logging.info("Calculate numbers for CoReport")
    df_coreport = calculation.calculate_numbers(df)
    # df_coreport =coreport_scraper.add_value_id(df_coreport, date=date)
    logging.info("Get value id's from CoReport")
    # with value id's already saved the day before:
    date = date.strftime('%d.%m.%Y')
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df_value_id = pd.read_pickle(file_name)
    # print(df_value_id)
    df_value_id["Hospital"] = hospital
    df_value_id.set_index("Hospital", inplace=True)
    df_coreport.set_index("Hospital", inplace=True)
    df_coreport = df_coreport.join(df_value_id)
    # print(df_coreport)
    # no need to filter since just one hospital in test case:
    # df_hospital = df_coreport.filter(items=[hospital], axis=0)
    df_hospital = df_coreport
    properties = update_coreport.get_properties_list(hospital=hospital[0])
    # print(properties)
    logging.info(f"Write entries into CoReport for {hospital}")
    for prop in properties:
        # value_id = credentials.dict_coreport[hospital][prop]
        value = int(df_hospital[prop][0])
        value_id = df_hospital[prop + " value_id"][0]
        # print(value_id, value)
        main_test(value_id=value_id, value=value)


def try_to_enter_in_coreport(date, day, list_hospitals, weekend):
    logging.info(f"Read out data for {day} in IES system")
    df, missing = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=weekend)
    if not df.empty:
        filled_hospitals = [x for x in list_hospitals if x not in missing]
        logging.info(f"Add entries of {filled_hospitals} for {day} into CoReport")
        write_in_coreport_test(df, filled_hospitals, date=date)
        logging.info(f"Entries added into CoReport for {filled_hospitals}")
        logging.info(f"There are no entries of {missing} for {day} in IES")
        if not not missing:
            for hospital in missing:
                logging.info(f"send email for missing entries {hospital} of {day}")
                # send_email.send_email(hospital=hospital, day=day)
    elif df.empty:
        logging.info(f"There are no entries for {day} in the IES system")
        for hospital in missing:
            logging.info(f"send email for missing entries {hospital} for {day}")
            # send_email.send_email(hospital=hospital, day=day)


def main_test(value_id, value, comment="Entered by bot"):
    # logging.basicConfig(level=logging.DEBUG)
    # logging.info(f'Executing {__file__}...')
    payload = {
        "value": value,
        "comment": comment
    }

    username = credentials.username_coreport_test
    password = credentials.password_coreport_test

    url = credentials.url_coreport + str(value_id)
    # print(url)

    r = common.requests_patch(url, json=payload,
                              auth=(username, password))
    r.raise_for_status()


def make_df_value_id(date):
    username = credentials.username_coreport_test
    password = credentials.password_coreport_test

    browser = mechanicalsoup.StatefulBrowser()
    browser.open(credentials.url_login_coreport)
    browser.select_form()
    # browser.form.print_summary()

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

    for data_name in properties_list:
        df[data_name + " value_id"] = ""
        if data_name == 'Bettenanzahl frei "Normalstation"' and data_time == '14.12.2021 10:00':
            value_id = 442320
        else:
            tag = browser.get_current_page().find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
            value_id = tag["id"].replace('form-', '')
        print(value_id)
        df.loc[0, data_name + " value_id"] = value_id
    # print(df)
    browser.close()
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df.to_pickle(file_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    # pd.set_option('display.max_columns', None)
    datum = datetime.today().date() - timedelta(3)
    run_test('Clara', datum)
    # make_df_value_id(date=datum)
    # df = pd.read_pickle('value_id_df_test_15.12.2021.pkl')
    # pd.set_option('display.max_columns', None)
    # print(df)
    print(df)