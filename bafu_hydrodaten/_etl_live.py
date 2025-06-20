from xml.etree import ElementTree

import requests

import common
from bafu_hydrodaten import credentials

print("Connecting to HTTPS Server to read data...")

local_path = "bafu_hydrodaten/data"
r = common.requests_get(
    url=credentials.https_live_url,
    auth=(credentials.https_user, credentials.https_pass),
)

print("Parsing response XML...")
root = ElementTree.fromstring(r.content)
# for child in root:
#     print(child.tag, child.attrib)

timestamp = root.find(".//*[@number='2289']/parameter[@type='2']/datetime").text
pegelstand = root.find(".//*[@number='2289']/parameter[@type='2']/value").text
abfluss = root.find(".//*[@number='2289']/parameter[@type='10']/value").text

print("current data: ")
print(f"Timestamp: {timestamp}")
print(f"Pegelstand: {pegelstand}")
print(f"Abfluss: {abfluss}")

print("Posting data to ods...")
payload = {"zeitstempel": timestamp, "pegel": pegelstand, "abflussmenge": abfluss}
r = requests.post(credentials.ods_test_push_api_url, json=payload)
print(f"Response status code: {r.status_code}")

# print(pegelstand.tag, pegelstand.attrib)
print("Processing data...")

print("Job successful!")
