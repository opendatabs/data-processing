from covid19_hosp import credentials
import common
import requests
import os
from bs4 import BeautifulSoup

print(f'Starting processing python script {__file__}...')
# use session to retain cookie infos
session = requests.Session()
# extract necessary info from the login form
login_form_url = credentials.hosp_domain + credentials.hosp_url_path
print(f'Getting content of login form at {login_form_url}...')
resp_loginform = session.get(login_form_url)
soup_login = BeautifulSoup(resp_loginform.content, 'html.parser')
# print(soup_login.prettify())
action_url = soup_login.find('form').get('action')
inputs = soup_login.find_all('input')
token = soup_login.find_all(attrs={"name": "csrfmiddlewaretoken"})[0].get('value')
next_url = soup_login.find_all(attrs={"name": "next"})[0].get('value')
action_url = soup_login.find(id='login-form').get('action')
# print(f'Cookies: {resp_loginform.cookies}')

login_form_action_url = credentials.hosp_domain + action_url
print(f'Posting login form to {login_form_action_url}...')
payload = dict(username=credentials.hosp_username,
               password=credentials.hosp_password,
               csrfmiddlewaretoken=token,
               next=next_url)

req_spital_bs = session.post(login_form_action_url, data=payload, headers=dict(Referer=login_form_url))
soup_spital_bs = BeautifulSoup(req_spital_bs.content, 'html.parser')
# print(soup_spital_bs.prettify())

for data_spec in credentials.hosp_data_files:
    print(f'Retrieving data from {data_spec["widget_id"]}...')
    # data_from_html = soup_spital_bs.find(id=data_spec['id']).text
    data_from_html = soup_spital_bs.find_all(attrs={'widget_id': data_spec['widget_id']})[0].text
    # print(f'{data_spec["id"]}: {data_from_html}')
    export_file_path = os.path.join(credentials.export_path, data_spec['filename'])
    print(f'Saving data to file {export_file_path}...')
    f = open(export_file_path, 'w')
    f.write(data_from_html)
    f.close()


