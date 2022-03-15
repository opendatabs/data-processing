from datetime import timezone, datetime, timedelta
from gsv_covid19_hosp_auto import get_data, send_email2
import mechanicalsoup
# from bs4 import BeautifulSoup
from gsv_covid19_hosp_auto import credentials
import logging
import common
import pandas as pd
from gsv_covid19_hosp_auto import hospitalzahlen
from gsv_covid19_hosp_auto import calculation
from gsv_covid19_hosp_auto import update_coreport
from zoneinfo import ZoneInfo


def run_test(list_hospitals, date):
    #list_hospitals = [hospital]
    # if after 10 and not done yet, save pickle file with value_id's for the next day:
    # Note: in actual case need to use make_df_value_id(date, list_hospitals) that gets value_ids for all hospitals in list_hospitals
    #if now_in_switzerland > time_for_email_final_status:
    #   datum = date + timedelta(1)
    #   # To do: only execute if pickle file not there yet
    #    make_df_value_id(date=datum)

    day_of_week = get_data.check_day(date)
    check_for_log_file(date, day_of_week, list_hospitals)
    df_log = pd.read_pickle("../log_file.pkl")
    if day_of_week == "Monday":
        condition = (df_log["Date"] == date - timedelta(2)) & (df_log['CoReport_filled'] != "Yes")
        hospitals_left_to_fill = df_log.loc[condition, "Hospital"]
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(2), day="Saturday", list_hospitals=hospitals_left_to_fill, weekend=True)
        condition = (df_log["Date"] == date - timedelta(1)) & (df_log['CoReport_filled'] != "Yes")
        hospitals_left_to_fill = df_log.loc[condition, "Hospital"]
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(1), day="Sunday", list_hospitals=hospitals_left_to_fill, weekend=True)
        condition = (df_log["Date"] == date) & (df_log['CoReport_filled'] != "Yes")
        hospitals_left_to_fill = df_log.loc[condition, "Hospital"]
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today", list_hospitals=hospitals_left_to_fill, weekend=False)

        # send emails if values missing for Saturday or Sunday
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(2), day="Saturday")
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(1), day="Sunday")
    elif day_of_week == "Other workday":
        condition = (df_log["Date"] == date) & (df_log['CoReport_filled'] != "Yes")
        hospitals_left_to_fill = df_log.loc[condition, "Hospital"]
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today", list_hospitals=hospitals_left_to_fill, weekend=False)
    else:
        logging.info("It is weekend")
    print(df_log)
    df_log = send_email2.check_if_email(df_log=df_log, date=date, day="today")
    df_log.to_pickle("log_file.pkl")
    print(df_log)
    df_log.to_csv("log_file.csv", index=False)

def check_for_log_file(date, day_of_week, list_hospitals):
    try:
        with open("../log_file.csv") as log_file:
            df_log = pd.read_csv(log_file)
            print( "log file")
            print(df_log)
            if str(date) not in list(df_log["Date"]):
                make_log_file(date, day_of_week, list_hospitals)
    except OSError:
        make_log_file(date, day_of_week, list_hospitals)


def make_log_file(date, day_of_week, list_hospitals):
    df = pd.DataFrame()
    numb_hosp = len(list_hospitals)
    if day_of_week == "Monday":
        df["Date"] = [date - timedelta(2)] * numb_hosp + [date - timedelta(1)] * numb_hosp + [date] * numb_hosp
        df["Hospital"] = list_hospitals * 3
    else:
        df["Date"] = [date] * numb_hosp
        df["Hospital"] = list_hospitals
    df['time_IES_entry'] = ""
    df['CoReport_filled'] = ""
    df['email_negative_value'] = ""
    df['email_reminder'] = ""
    df['email_for_calling'] = ""
    df['email_status_at_10'] = ""
    df['email_all_filled'] = ""
    df['all_filled'] = 0
    #df.set_index("Date", inplace=True)
    df.to_pickle("log_file.pkl")


def try_to_enter_in_coreport(df_log, date, day, list_hospitals, weekend):
    logging.info(f"Read out data for {day} in IES system")
    df, missing = hospitalzahlen.get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=weekend)
    if not df.empty:
        filled_hospitals = [x for x in list_hospitals if x not in missing]
        logging.info(f"Add entries of {filled_hospitals} for {day} into CoReport")
        df_log = write_in_coreport_test(df, filled_hospitals,date=date, day=day, df_log=df_log)
        for hospital in filled_hospitals:
            row_hospital = df[df["Hospital"] == hospital]
            timestamp = row_hospital["CapacTime"].values[0]
            condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
            print("condition")
            print(condition)
            df_log.loc[condition, 'time_IES_entry'] = timestamp
            print(df_log)
        logging.info(f"There are no entries of {missing} for {day} in IES")
    return df_log


def write_in_coreport_test(df, hospital_list, date, day, df_log, current_time= datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich')).time().replace(microsecond=0)):
    logging.info("Calculate numbers for CoReport")
    df_coreport = calculation.calculate_numbers(df)
    print(df_log)
    # df_coreport =coreport_scraper.add_value_id(df_coreport, date=date)
    logging.info("Get value id's from CoReport")
    # with value id's already saved the day before:
    datum = date.strftime('%d.%m.%Y')
    file_name = "value_id_df_test_" + str(datum) + ".pkl"
    df_value_id = pd.read_pickle(file_name)
    # print(df_value_id)
    for hospital in hospital_list:
        logging.info(f"Write entries into CoReport for {hospital}")
        #df_value_id.set_index("Hospital", inplace=True)
        #df_coreport.set_index("Hospital", inplace=True)
        # df_coreport = df_coreport.join(df_value_id)
        index_hospital = df_coreport.index[df_coreport["Hospital"] == hospital]
        df_hospital = df_coreport[df_coreport["Hospital"] == hospital]
        df_hospital = df_hospital.join(df_value_id)
        properties = update_coreport.get_properties_list(hospital=hospital)
        # print(properties)
        logging.info(f"Write entries into CoReport for {hospital}")
        incomplete = 0
        for prop in properties:
            # value_id = credentials.dict_coreport[hospital][prop]
            value = int(df_hospital[prop][index_hospital])
            value_id = df_hospital[prop + " value_id"][index_hospital]
            # quick fix to ignore negative values
            if value >= 0:
                main_test(value_id=value_id, value=value)
                logging.info(f"Added {value} for {prop} of {hospital} ")
            else:
                logging.warning(f"Negative value for {prop} of {hospital}! send email...")
                condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                incomplete += 1
                if (df_log.loc[condition, 'email_negative_value'] == "").all():
                    send_email2.send_email(hospital=hospital, email_type="Negative value", day=day, extra_info=[prop, hospital])
                    df_log.loc[condition, 'email_negative_value'] = f"Sent at {current_time}"
        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
        if incomplete == 0:
            df_log.loc[condition, 'CoReport_filled'] = "Yes"
            logging.info(f"Entries added into CoReport for {hospital}")
        else:
            df_log.loc[condition, 'CoReport_filled'] = "Not all filled"
            logging.warning(f"Entries only partly added into CoReport for {hospital}")
    return df_log

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
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    date = now_in_switzerland.date()
    time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=30, tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_to_call = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=50, tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_final_status = datetime(year=date.year, month=date.month, day=date.day, hour=10, minute=0, tzinfo=ZoneInfo('Europe/Zurich'))
    pd.set_option('display.max_columns', None)
    datum = datetime.today().date() - timedelta(1)
    run_test(['Clara','UKBB', 'USB'], datum)
    # make_df_value_id(date=datum)
    # df = pd.read_pickle('value_id_df_test_15.01.2022.pkl')
    # pd.set_option('display.max_columns', None)
    # print(df)

