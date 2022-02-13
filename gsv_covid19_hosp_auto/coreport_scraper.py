import datetime
import pandas as pd
from gsv_covid19_hosp_auto import credentials
from bs4 import BeautifulSoup
import mechanicalsoup
import logging


def make_df_value_id(date, list_hospitals):
    username = credentials.username_coreport
    password = credentials.password_coreport

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
    df["Hospital"] = list_hospitals
    df.set_index("Hospital", inplace=True)

    for data_name in properties_list:
        df[data_name + " value_id"] = ""

    for hospital in list_hospitals:
        if hospital == 'Clara':
            response = browser.get(credentials.url_coreport_clara)
            response.raise_for_status()
            data_names = [x for x in properties_list if
                          x not in ['Bettenanzahl frei " IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"']]
        elif hospital == 'USB':
            response = browser.get(credentials.url_coreport_usb)
            response.raise_for_status()
            data_names = properties_list
        elif hospital == 'UKBB':
            response = browser.get(credentials.url_coreport_ukbb)
            response.raise_for_status()
            data_names = [x for x in properties_list if
                          x not in ['Bettenanzahl frei " IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"']]

        soup = BeautifulSoup(response.text, 'html.parser')
        for data_name in data_names:
            tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
            value_id = tag["id"].replace('form-', '')
            df.loc[hospital, data_name + " value_id"] = value_id
    browser.close()
    file_name = "value_id_df_" + str(date) + ".pkl"
    df.to_pickle(file_name)


def add_value_id(df, date):
    username = credentials.username_coreport
    password = credentials.password_coreport

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

    columns = list(df.columns[4:])
    for data_name in columns:
        df[data_name + " value_id"] = ""
    hospitals = list(df["Hospital"])
    df.set_index("Hospital", inplace=True)
    for hospital in hospitals:
        if hospital == 'Clara':
            response = browser.get(credentials.url_coreport_clara)
            response.raise_for_status()
            data_names = [x for x in columns if x not in ['Bettenanzahl frei " IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"']]
        elif hospital == 'USB':
            response = browser.get(credentials.url_coreport_usb)
            response.raise_for_status()
            data_names = columns
        elif hospital == 'UKBB':
            response = browser.get(credentials.url_coreport_ukbb)
            response.raise_for_status()
            data_names = [x for x in columns if x not in ['Bettenanzahl frei " IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"']]
        soup = BeautifulSoup(response.text, 'html.parser')
        for data_name in data_names:
            tags = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})
            # Check if tag for the respective combination is present at all
            if len(tags) > 0:
                tag = tags[0]
                value_id = tag["id"].replace('form-', '')
                df.loc[hospital, data_name + " value_id"] = value_id
            else:
                logging.warning(f"No tag for {data_name} and {data_time} present!")
    browser.close()
    return df


if __name__ == "__main__":
    list_hospitals = ['USB', 'Clara', 'UKBB']
    date = datetime.datetime.today().date() #- datetime.timedelta(1)
    make_df_value_id(date=date, list_hospitals=list_hospitals)



