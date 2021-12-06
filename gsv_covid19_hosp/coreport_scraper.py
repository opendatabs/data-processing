from gsv_covid19_hosp import credentials
import common
import requests
import os
from bs4 import BeautifulSoup
import mechanicalsoup

username = credentials.username_coreport
password = credentials.password_coreport

browser = mechanicalsoup.StatefulBrowser()
browser.open(credentials.url_login_coreport)
browser.select_form()
browser.form.print_summary()


browser["login"] = username
browser["password"] = password


browser.submit_selected()


response = browser.get(credentials.url_coreport_clara)

soup = BeautifulSoup(response.text, 'html.parser')


#print(soup.find_all(data-name='Bettenanzahl frei "Normalstation"'))
#print(soup.find_all('form'))

#print(soup.find_all('form', 'slot slot-form'))

#print(soup.prettify())


list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name})[0]
    value_id = tag["id"].replace('form-', '')
    print(value_id)

response = browser.get(credentials.url_coreport_ukbb)
soup = BeautifulSoup(response.text, 'html.parser')
list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name})[0]
    value_id = tag["id"].replace('form-', '')
    print(value_id)


response = browser.get(credentials.url_coreport_usb)
soup = BeautifulSoup(response.text, 'html.parser')
list_of_data_names = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"']

list_of_data_names.append(['Bettenanzahl frei "IPS ECMO"', 'Bettenanzahl belegt "IPS ECMO"'])

for data_name in list_of_data_names:
    tag = soup.find_all(attrs={'data-name': data_name})[0]
    value_id = tag["id"].replace('form-', '')
    print(value_id)


browser.close()

