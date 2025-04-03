import ods_utils_py as ods_utils 
import pandas as pd 
import json 
from bs4 import BeautifulSoup
import re
from ods_check_urls import credentials
import os


data = ods_utils.get_dataset_metadata(dataset_id=100404)

### --- 1. Extract URLs outside of "description" --- ###
# URL-Regex (http, https, ftp, www., domain.tld/pfad)
url_pattern = re.compile(r"""(?xi)
\b(?:https?|ftp)://[^\s"'<>]+
|
www\.[^\s"'<>]+
|
(?:[a-z0-9\-]+\.)+[a-z]{2,}(?:/[^\s"'<>]*)?
""", re.VERBOSE)

# --- Extract all URLs outside "description" ---
def find_urls_excluding_description(obj):
    urls = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "description":
                continue
            elif k == "references" and isinstance(v, str):
                refs = [ref.strip().strip(';') for ref in v.split(';')]
                urls.extend(refs)
            else:
                urls.extend(find_urls_excluding_description(v))
    elif isinstance(obj, list):
        for item in obj:
            urls.extend(find_urls_excluding_description(item))
    elif isinstance(obj, str):
        if "@" in obj and re.match(r".+@.+\..+", obj):  # is an E-Mail
            return []  # skip
        found = url_pattern.findall(obj)
        urls.extend([u.strip().strip(';') for u in found])
    return urls

# --- Extract URLs from description ( <a href="...">) ---
def extract_hrefs_from_description(html_str):
    soup = BeautifulSoup(html_str, "html.parser")
    return [a["href"].strip().strip(';') for a in soup.find_all("a", href=True)]

description_html = data.get("default", {}).get("description", {}).get("value", "")
description_urls = extract_hrefs_from_description(description_html)

# --- Combine & clean all URLs ---
other_urls = find_urls_excluding_description(data)
all_raw_urls = sorted(set(description_urls + other_urls))

# --- Add URL (http://...) if no protocol is availablet ---
def normalize_url(url):
    if re.match(r'^(https?|ftp)://', url, re.I):
        return url
    return "http://" + url

# --- Check validity ---
def check_url(url):
    try:
        resp = ods_utils.requests_get(url, allow_redirects=True)
        return resp.status_code
    except Exception as e:
        return str(e.__class__.__name__)

# --- Check and save results---
results = []
for raw in all_raw_urls:
    test_url = normalize_url(raw)
    status = check_url(test_url)
    results.append({
        "Original": raw,
        "Geprüfte URL": test_url,
        "Status": status
    })

# --- Save data frame  ---
df = pd.DataFrame(results)
file_path = os.path.join(credentials.data_path,'Liste.xlsx')
df.to_excel(file_path)

