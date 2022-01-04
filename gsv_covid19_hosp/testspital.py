from datetime import timezone, datetime, timedelta
from gsv_covid19_hosp import get_data
import mechanicalsoup
# from bs4 import BeautifulSoup
from gsv_covid19_hosp import credentials
import logging
import common
import pandas as pd
import threading
from gsv_covid19_hosp import hospitalzahlen
from gsv_covid19_hosp import calculation
from gsv_covid19_hosp import update_coreport
from zoneinfo import ZoneInfo


now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
print(now_in_switzerland)

date = now_in_switzerland.date()


time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=15).astimezone(ZoneInfo('Europe/Zurich'))
print(time_for_email)


def run_test(hospital, date):
    list_hospitals = [hospital]
    day_of_week = get_data.check_day(date)
    if day_of_week == "Monday":
        saturday = date-timedelta(2)
        logging.info("Read out data from Saturday in IES system")
        df_saturday, missing_saturday = hospitalzahlen.get_df_for_date(date=saturday, list_hospitals=list_hospitals, weekend=True)
        if not df_saturday.empty:
            list_hospitals_sat = [x for x in list_hospitals if x not in missing_saturday]
            logging.info(f"Add Saturday entries of {list_hospitals_sat} into CoReport")
            write_in_coreport_test(df_saturday, list_hospitals_sat, date=saturday)
            logging.info(f"Entries added into CoReport for {list_hospitals_sat}")
            logging.info(f"There are no entries on Saturday for {missing_saturday} in IES")
            if not not missing_saturday:
                for hospital in missing_saturday:
                    logging.info(f"send email for missing entries {hospital} on Saturday")
                    #send_email.send_email(hospital=hospital, day="Saturday")
        elif df_saturday.empty:
            logging.info(f"There are no entries on Saturday in the IES system")
            for hospital in missing_saturday:
                logging.info(f"send email for missing entries {hospital} on Saturday")
                #send_email.send_email(hospital=hospital, day="Saturday")
        sunday = date - timedelta(1)
        df_sunday, missing_sunday = hospitalzahlen.get_df_for_date(date=sunday, list_hospitals=list_hospitals, weekend=True)
        if not df_sunday.empty:
            list_hospitals_sun = [x for x in list_hospitals if x not in missing_sunday]
            logging.info(f"Add Sunday entries of {list_hospitals_sun} into CoReport")
            write_in_coreport_test(df_sunday, list_hospitals_sun, date=sunday)
            logging.info(f"Entries added into CoReport for {list_hospitals_sun}")
            logging.info(f"There are no entries on Sunday for {missing_sunday} in IES")
            if not not missing_sunday:
                for hospital in missing_sunday:
                    logging.info(f"send email for missing entries {hospital} on Sunday")
                    #send_email.send_email(hospital=hospital, day="Sunday")
        elif df_sunday.empty:
            logging.info(f"There are no entries on Sunday in the IES system")
            for hospital in missing_sunday:
                logging.info(f"send email for missing entries {hospital} on Sunday")
                #send_email.send_email(hospital=hospital, day="Sunday")
        df_monday, missing_hospitals = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if not df_monday.empty:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            logging.info(f"Add today's entries of {filled_hospitals} into CoReport")
            write_in_coreport_test(df_monday, filled_hospitals, date=date)
            logging.info(f"Entries added into CoReport for {filled_hospitals}")
            logging.info(f"There are no entries today for {missing_hospitals} in IES")
            if not not missing_hospitals and now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    #send_email.send_email(hospital=hospital)
        elif df_monday.empty:
            logging.info(f"There are no entries in the IES system")
            if now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    #send_email.send_email(hospital=hospital)
    elif day_of_week == "Other workday":
        df, missing_hospitals = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if not df.empty:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            logging.info(f"Add today's entries of {filled_hospitals} into CoReport")
            write_in_coreport_test(df, filled_hospitals, date=date)
            logging.info(f"Entries added into CoReport for {filled_hospitals}")
            logging.info(f"There are no entries today for {missing_hospitals} in IES")
            if not not missing_hospitals and now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    #send_email.send_email(hospital=hospital)
        elif df.empty:
            logging.info("There are no entries today in IES")
            if now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    #send_email.send_email(hospital=hospital)
    else:
        logging.info("It is weekend")


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
    #print(url)

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
    #print(df)
    browser.close()
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df.to_pickle(file_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    # pd.set_option('display.max_columns', None)
    datum = datetime.today().date() #+ timedelta(1)
    run_test('Clara', datum)
    # make_df_value_id(date=datum)
    # df = pd.read_pickle('value_id_df_test_15.12.2021.pkl')
    # pd.set_option('display.max_columns', None)
    # print(df)
    # print(df['Bettenanzahl frei " IPS ECMO" value_id'][0])
