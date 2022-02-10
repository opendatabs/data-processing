from gsv_covid19_hosp_auto import credentials
import common
import requests


url_api = credentials.url_coreport_test_api

filter = '&organization=claraspital-erweitert&timeslot=20-12-2021&question=Bettenanzahl+belegt+%22IPS+mit+Beatmung%22'
username = credentials.username_coreport
password = credentials.password_coreport
payload = {}

url = url_api + filter


req = requests.get(url, auth=(username, password))

file =req.json()
print(file)