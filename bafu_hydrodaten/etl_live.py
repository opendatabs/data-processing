import json
from xml.etree import ElementTree

import pandas as pd
import requests

from bafu_hydrodaten import credentials

print(f'Connecting to HTTPS Server to read data...')

local_path = 'bafu_hydrodaten/data'
r = requests.get(credentials.https_live_url, auth=(credentials.https_user, credentials.https_pass))

print(f'Parsing response XML...')
root = ElementTree.fromstring(r.content)
# for child in root:
#     print(child.tag, child.attrib)

timestamp = root.find(".//*[@number='2289']/parameter[@type='2']/datetime").text
pegelstand = root.find(".//*[@number='2289']/parameter[@type='2']/value").text
abfluss = root.find(".//*[@number='2289']/parameter[@type='10']/value").text

print(f'current data: ')
print(f'Timestamp: {timestamp}')
print(f'Pegelstand: {pegelstand}')
print(f'Abfluss: {abfluss}')

print(f'Posting data to ods...')
payload = {'zeitstempel': timestamp, 'pegel': pegelstand, 'abflussmenge': abfluss}
r = requests.post(credentials.ods_push_api_url, json=payload)
print(f'Response status code: {r.status_code}')

#print(pegelstand.tag, pegelstand.attrib)
print('Processing data...')

print('Job successful!')
