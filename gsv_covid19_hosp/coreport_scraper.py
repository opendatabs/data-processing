import datetime

from gsv_covid19_hosp import credentials
import common
import requests
import os
from bs4 import BeautifulSoup
import mechanicalsoup


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
    print(df["Hospital"])
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
            print(hospital, data_name, data_time)
            tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
            value_id = tag["id"].replace('form-', '')
            print(value_id)
            print(df.loc[hospital, data_name + " value_id"])
            df.loc[hospital, data_name + " value_id"] = value_id
    browser.close()
    return df



"""
today = datetime.datetime.today().date() + datetime.timedelta(1)
datum = today.strftime('%d.%m.%Y')
data_time = datum + " 10:00"
list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
    value_id = tag["id"].replace('form-', '')
    time = tag["data-time"]
    print(value_id, time)

response = browser.get(credentials.url_coreport_ukbb)
soup = BeautifulSoup(response.text, 'html.parser')
html = soup.prettify("utf-8")
# with open ("outputUKBB.html", "wb") as file:
#     file.write(html)

list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
    value_id = tag["id"].replace('form-', '')
    time = tag["data-time"]
    print(value_id, time)


response = browser.get(credentials.url_coreport_usb)
soup = BeautifulSoup(response.text, 'html.parser')
html = soup.prettify("utf-8")
# with open ("outputUSB.html", "wb") as file:
#     file.write(html)


list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

list_of_data_names.append(['Bettenanzahl frei "IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"'])

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name, 'data-time': data_time})[0]
    value_id = tag["id"].replace('form-', '')
    time = tag["data-time"]
    print(value_id, time)
"""



