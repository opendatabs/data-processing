import datetime

import mechanicalsoup
from bs4 import BeautifulSoup
import credentials
import logging
import common
import pandas as pd
import numpy as np
from math import nan



def main(value_id, value):

    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    payload = {
        "value": value,
        "comment": "Entered by bot"
}

    #value_id = '422640'

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
    html = soup.prettify("utf-8")
    with open('outputTest.html', "wb") as file:
        file.write(html)
    tag_date = soup.find_all(attrs={'data-time': data_time})
    #print(tag_date)
    tag_ecmo = soup.find_all(attrs={'data-name': 'Bettenanzahl frei " IPS ECMO"'})
    print(tag_ecmo)
    tag_betten = soup.find_all(attrs={'data-name': 'Bettenanzahl frei "IMCU"'})
    print(tag_betten)
    #print(soup.prettify())
    for data_name in properties_list:
        df[data_name + " value_id"] = ""
        if data_name == 'Bettenanzahl frei "Normalstation"' and data_time == '14.12.2021 10:00':
            value_id = 442320
        else:
            tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
            value_id = tag["id"].replace('form-', '')
        print(value_id)
        df.loc[0,data_name + " value_id"] = value_id
    browser.close()
    file_name = "value_id_df_test_" + str(date) + ".pkl"
    df.to_pickle(file_name)


if __name__ == "__main__":
    date = datetime.datetime.today().date() + datetime.timedelta(1)
    make_df_value_id(date=date)
    df = pd.read_pickle('value_id_df_test_14.12.2021.pkl')
    #pd.set_option('display.max_columns', None)
    #print(df)
    #print(df['Bettenanzahl frei " IPS ECMO" value_id'][0])
