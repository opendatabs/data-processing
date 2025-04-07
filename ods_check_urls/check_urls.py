import ods_utils_py as ods_utils
import pandas as pd
from bs4 import BeautifulSoup
import re
from ods_check_urls import credentials
import os
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
import logging


# URL extraction functions
def find_urls_excluding_description(obj):
        # URL extraction patterns
    url_pattern = re.compile(r"""(?xi)
    \b(?:https?|ftp)://[^\s"'<>]+
    |
    www\.[^\s"'<>]+
    |
    (?:[a-z0-9\-]+\.)+[a-z]{2,}(?:/[^\s"'<>]*)?
    """, re.VERBOSE)

    urls = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("description", "visualization", "tags"):
                continue
            elif k == "references" and isinstance(v, str):
                refs = [ref.strip().strip(";") for ref in v.split(";")]
                urls.extend(refs)
            else:
                urls.extend(find_urls_excluding_description(v))
    elif isinstance(obj, list):
        for item in obj:
            urls.extend(find_urls_excluding_description(item))
    elif isinstance(obj, str):
        if "@" in obj and re.match(r".+@.+\..+", obj):
            return []
        found = url_pattern.findall(obj)
        urls.extend([u.strip().strip(";") for u in found])
    return urls

def extract_hrefs_from_html(html_str):
    soup = BeautifulSoup(html_str, "html.parser")
    return [a["href"].strip().strip(";") for a in soup.find_all("a", href=True)]

def normalize_url(url):
    if re.match(r"^(https?|ftp)://", url, re.I):
        return url
    return "http://" + url

# URL check: GET + Selenium fallback
def check_url_selenium(url):
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(15)
        driver.get(url)
        driver.quit()
        return "OK"
    except WebDriverException as e:
        return type(e).__name__
    except Exception as e:
        return str(e)

def check_url(url):
    
    try:
        #resp = ods_utils.requests_get(url, allow_redirects=True)
        resp = requests.get(url=url, allow_redirects=True)
        return resp.status_code
    except Exception:
        return check_url_selenium(url)
    

def main():
    CSV_URL = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100055/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true"
    urls_file = os.path.join(credentials.data_path, "broken_urls.xlsx")
    dataset_fetch_errors = os.path.join(credentials.data_path, "dataset_fetch_errors.xlsx")
    # Load dataset IDs from CSV
    response = ods_utils.requests_get(CSV_URL)
    response.raise_for_status()
    df_csv = pd.read_csv(StringIO(response.text), delimiter=";")
    dataset_ids = df_csv["dataset_id"].tolist()
    # Collect all URLs across datasets
    df_urls = pd.DataFrame(columns=["url", "dataset_id"])
    datasets_checked = 0
    # List to store dataset_id and error message for failed fetch attempts
    errors_dataset = []
    for dataset_id in tqdm(dataset_ids, desc="Collecting URLs"):
        try:
            data = ods_utils.get_dataset_metadata(dataset_id=dataset_id)
            datasets_checked += 1  # ✅ Count only successful fetches
        except Exception as e:
            errors_dataset.append({
            "dataset_id": dataset_id,
            "error": str(e)
            })
            continue

        # Extract <a href="..."> links from both description and custom_view_html
        desc_html = data.get("default", {}).get("description", {}).get("value", "")
        #vis_html = data.get("visualization", {}).get("custom_view_html", {}).get("value", "")
        html_urls = extract_hrefs_from_html(desc_html)

        # Extract everything else via regex
        other_urls = find_urls_excluding_description(data)

        all_urls = set(html_urls + other_urls)

        df_temp = pd.DataFrame({
            "url": list(all_urls),
            "dataset_id": dataset_id
        })
        df_urls = pd.concat([df_urls, df_temp], ignore_index=True)

    # Normalize and group URLs
    df_urls["url"] = df_urls["url"].apply(normalize_url)
    df_grouped = df_urls.groupby("url")["dataset_id"].apply(lambda ids: list(sorted(set(ids)))).reset_index()
    # List to store all URLs that failed during the check
    results = []# Store dataset_id and error message for failed fetch attempts
    for _, row in tqdm(df_grouped.iterrows(), total=len(df_grouped), desc="Checking URLs"):
        url = row["url"]
        status = check_url(url)

        if str(status) not in ("200", "OK"):
            results.append({
                "url": url,
                "dataset_ids": ", ".join(map(str, row["dataset_id"])),
                "status": status
            })

    # Export broken URLs to Excel

    df_errors = pd.DataFrame(results)
    df_errors.to_excel(urls_file, index=False)
    logging.info(f"Total datasets checked: {datasets_checked}")
    logging.info(f"Broken URLs saved to: {urls_file}")
    if errors_dataset:
        df_failed = pd.DataFrame(errors_dataset)
        df_failed.to_excel(dataset_fetch_errors, index=False)
        logging.info(f"{len(errors_dataset)} datasets could not be loaded. See 'dataset_fetch_errors.xlsx'.")

    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
    