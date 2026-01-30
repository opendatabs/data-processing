import logging
import os
import shutil
from pathlib import Path
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import common
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DATA_ORIG_PATH = "data_orig"
DOKUMENTE_PATH = os.path.join(DATA_ORIG_PATH, "Dokumente")
TEXTRUECKMELDUNGEN_PATH = os.path.join(DATA_ORIG_PATH, "Textrueckmeldungen")

BASE_URL = "https://www.bs.ch"
VERNEHMLASSUNGEN_URL = "https://www.bs.ch/regierungsrat/vernehmlassungen#abgeschlossene-vernehmlassungen"

# URLs for the 4 pages with abgeschlossene Vernehmlassungen
VERNEHMLASSUNGEN_PAGES = [
    "https://www.bs.ch/regierungsrat/vernehmlassungen/abgeschlossene-vernehmlassungen-2023-2025",
    "https://www.bs.ch/regierungsrat/vernehmlassungen/abgeschlossene-vernehmlassungen-2020-2022",
    "https://www.bs.ch/regierungsrat/vernehmlassungen/abgeschlossene-vernehmlassungen-2017-2019",
    "https://www.bs.ch/regierungsrat/vernehmlassungen/abgeschlossene-vernehmlassungen-2014-2016",
]


def sanitize_filename(name: str) -> str:
    """Sanitize filename for FTP upload."""
    transl_table = str.maketrans({"ä": "ae", "Ä": "Ae", "ö": "oe", "Ö": "Oe", "ü": "ue", "Ü": "Ue", "ß": "ss"})
    name = name.translate(transl_table).replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return "".join(c for c in name if c in allowed)


def parse_german_date(date_str: str) -> str:
    """Parse German date format (e.g., '23. Juni 2023') and convert to yyyy-mm-dd."""
    # Handle NaN, None, or empty values
    if pd.isna(date_str) or date_str is None:
        return ""
    
    # Convert to string if not already
    date_str = str(date_str)
    
    if not date_str or not date_str.strip() or date_str.lower() in ['nan', 'none', '']:
        return ""
    
    # German month names mapping
    german_months = {
        "januar": "01", "februar": "02", "märz": "03", "april": "04",
        "mai": "05", "juni": "06", "juli": "07", "august": "08",
        "september": "09", "oktober": "10", "november": "11", "dezember": "12"
    }
    
    try:
        # Remove any leading/trailing whitespace
        date_str = date_str.strip()
        
        # Pattern: "DD. Month YYYY" or "D. Month YYYY" or "DD Month YYYY" (with or without period)
        pattern = r"(\d{1,2})\.?\s+(\w+)\s+(\d{4})"
        match = re.match(pattern, date_str, re.IGNORECASE)
        
        if match:
            day = match.group(1).zfill(2)  # Pad with zero if needed
            month_name = match.group(2).lower()
            year = match.group(3)
            
            # Get month number
            month = german_months.get(month_name)
            if not month:
                # Try alternative spellings or abbreviations
                month_alternatives = {
                    "märz": "03", "maerz": "03", "mrz": "03",
                    "mär": "03", "maer": "03"
                }
                month = month_alternatives.get(month_name, "")
            
            if month:
                return f"{year}-{month}-{day}"
            else:
                logging.warning(f"Could not parse month '{month_name}' from date '{date_str}'")
                return date_str  # Return original if parsing fails
        else:
            # Try to parse with datetime if it's already in a standard format
            try:
                # Try various formats
                for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"]:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            except Exception:
                pass
            
            logging.warning(f"Could not parse date format: '{date_str}'")
            return date_str  # Return original if parsing fails
            
    except Exception as e:
        logging.error(f"Error parsing date '{date_str}': {e}")
        return date_str  # Return original on error


def fetch_page(url: str) -> BeautifulSoup:
    """Fetch and parse a webpage."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")


def extract_dates_from_text(text: str) -> tuple[str, str]:
    """Extract start and end dates from text. Returns (startdatum, enddatum) as yyyy-mm-dd."""
    if not text:
        return "", ""
    
    startdatum = ""
    enddatum = ""
    
    # Pattern 1: "Vernehmlassung: DD. Month YYYY - DD. Month YYYY" (with colon)
    pattern1 = r"Vernehmlassung:\s*(\d{1,2}\.?\s+\w+\s+\d{4})\s*-\s*(\d{1,2}\.?\s+\w+\s+\d{4})"
    match1 = re.search(pattern1, text, re.IGNORECASE)
    if match1:
        startdatum = parse_german_date(match1.group(1))
        enddatum = parse_german_date(match1.group(2))
        return startdatum, enddatum
    
    # Pattern 1b: "Vernehmlassung: DD. Month - DD. Month YYYY" (first date missing year)
    pattern1b = r"Vernehmlassung:\s*(\d{1,2}\.?\s+(\w+))\s*-\s*(\d{1,2}\.?\s+(\w+)\s+(\d{4}))"
    match1b = re.search(pattern1b, text, re.IGNORECASE)
    if match1b:
        # Extract components
        first_day = match1b.group(1).split()[0].replace(".", "")
        first_month_str = match1b.group(2).lower()
        second_date_str = match1b.group(3)
        second_month_str = match1b.group(4).lower()
        year = int(match1b.group(5))
        
        # German month numbers
        german_months = {
            "januar": 1, "februar": 2, "märz": 3, "april": 4,
            "mai": 5, "juni": 6, "juli": 7, "august": 8,
            "september": 9, "oktober": 10, "november": 11, "dezember": 12
        }
        
        # Determine year for first date
        # If first month is after second month (e.g., Dezember before März), use previous year
        first_month_num = german_months.get(first_month_str, 0)
        second_month_num = german_months.get(second_month_str, 0)
        
        if first_month_num > second_month_num:
            # First date is in previous year (e.g., Dezember 2020 - März 2021)
            first_year = year - 1
        else:
            # Same year
            first_year = year
        
        first_date_str = f"{first_day}. {first_month_str.capitalize()} {first_year}"
        startdatum = parse_german_date(first_date_str)
        enddatum = parse_german_date(second_date_str)
        return startdatum, enddatum
    
    # Pattern 2: "Vernehmlassung: DD. Month YYYY bis DD. Month YYYY" (with colon)
    pattern2 = r"Vernehmlassung:\s*(\d{1,2}\.?\s+\w+\s+\d{4})\s+bis\s+(\d{1,2}\.?\s+\w+\s+\d{4})"
    match2 = re.search(pattern2, text, re.IGNORECASE)
    if match2:
        startdatum = parse_german_date(match2.group(1))
        enddatum = parse_german_date(match2.group(2))
        return startdatum, enddatum
    
    # Pattern 3: "Vernehmlassung DD. Month YYYY bis DD. Month YYYY" (without colon)
    pattern3 = r"Vernehmlassung\s+(\d{1,2}\.?\s+\w+\s+\d{4})\s+bis\s+(\d{1,2}\.?\s+\w+\s+\d{4})"
    match3 = re.search(pattern3, text, re.IGNORECASE)
    if match3:
        startdatum = parse_german_date(match3.group(1))
        enddatum = parse_german_date(match3.group(2))
        return startdatum, enddatum
    
    # Pattern 4: "Vernehmlassung DD. Month YYYY - DD. Month YYYY" (without colon, with dash)
    pattern4 = r"Vernehmlassung\s+(\d{1,2}\.?\s+\w+\s+\d{4})\s*-\s*(\d{1,2}\.?\s+\w+\s+\d{4})"
    match4 = re.search(pattern4, text, re.IGNORECASE)
    if match4:
        startdatum = parse_german_date(match4.group(1))
        enddatum = parse_german_date(match4.group(2))
        return startdatum, enddatum
    
    # Pattern 5: Just dates without "Vernehmlassung" prefix
    pattern5 = r"(\d{1,2}\.?\s+\w+\s+\d{4})\s*-\s*(\d{1,2}\.?\s+\w+\s+\d{4})"
    match5 = re.search(pattern5, text, re.IGNORECASE)
    if match5:
        startdatum = parse_german_date(match5.group(1))
        enddatum = parse_german_date(match5.group(2))
        return startdatum, enddatum
    
    return "", ""


def extract_vernehmlassungen_from_page(soup: BeautifulSoup, page_url: str) -> list[dict]:
    """Extract Vernehmlassungen information from a page."""
    vernehmlassungen = []
    
    # Find all h2 headings which are Vernehmlassung titles
    headings = soup.find_all("h2", class_=lambda x: x and "container" in x and "paragraph--margin" in x)
    
    for heading in headings:
        name = heading.get_text(strip=True)
        if not name:
            continue
        
        entry = {"name_vernehmlassung": name}
        
        # Find the next div with content after the heading
        current = heading.find_next_sibling("div")
        startdatum = ""
        enddatum = ""
        beschreibung = ""
        
        # Look for the text content div
        while current:
            if hasattr(current, "get_text"):
                text_content = current.get_text()
                
                # Try to extract dates from this text
                extracted_start, extracted_end = extract_dates_from_text(text_content)
                if extracted_start and not startdatum:
                    startdatum = extracted_start
                if extracted_end and not enddatum:
                    enddatum = extracted_end
                
                # Extract description (text after the full date line, not including any date)
                if extracted_start or extracted_end:
                    # Match the full date line (both dates) so we cut after it
                    full_date_patterns = [
                        r"Vernehmlassung:?\s*\d{1,2}\.?\s+\w+\s+\d{4}\s*-\s*\d{1,2}\.?\s+\w+\s+\d{4}",
                        r"Vernehmlassung:?\s*\d{1,2}\.?\s+\w+\s+\d{4}\s+bis\s+\d{1,2}\.?\s+\w+\s+\d{4}",
                    ]
                    date_end_pos = 0
                    for pat in full_date_patterns:
                        m = re.search(pat, text_content, re.IGNORECASE)
                        if m:
                            date_end_pos = m.end()
                            break
                    if date_end_pos > 0:
                        beschreibung = text_content[date_end_pos:].strip()
                    else:
                        beschreibung = re.sub(r"Vernehmlassung:?\s*\d{1,2}\.?\s+\w+\s+\d{4}\s*-\s*\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", text_content, count=1, flags=re.IGNORECASE)
                        beschreibung = re.sub(r"Vernehmlassung:?\s*\d{1,2}\.?\s+\w+\s+\d{4}\s+bis\s+\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", beschreibung, count=1, flags=re.IGNORECASE)
                        beschreibung = beschreibung.strip()
                    # Strip leading end-date fragments that sometimes appear ("- DD. Month YYYY" or "bis DD. Month YYYY")
                    beschreibung = re.sub(r"^-\s*\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", beschreibung, flags=re.IGNORECASE)
                    beschreibung = re.sub(r"^bis\s+\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", beschreibung, flags=re.IGNORECASE)
                    beschreibung = beschreibung.strip()
                elif not beschreibung and len(text_content) > 50:
                    # If no date found but there's substantial text, use it as description
                    beschreibung = text_content.strip()
                    # Still strip any leading date fragments
                    beschreibung = re.sub(r"^-\s*\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", beschreibung, flags=re.IGNORECASE)
                    beschreibung = re.sub(r"^bis\s+\d{1,2}\.?\s+\w+\s+\d{4}\s*", "", beschreibung, flags=re.IGNORECASE)
                    beschreibung = beschreibung.strip()

                # Clean up description - remove extra whitespace
                if beschreibung:
                    beschreibung = re.sub(r'\s+', ' ', beschreibung)
            
            # Check if we've hit the next heading or link list
            if hasattr(current, "name"):
                if current.name == "h2":
                    break
                # Stop at link lists (document sections)
                if current.name in ["ul", "div"] and current.find("a", href=True):
                    links = current.find_all("a", href=True)
                    if any(".pdf" in link.get("href", "").lower() or "media.bs.ch" in link.get("href", "") for link in links):
                        break
            
            current = current.find_next_sibling()
        
        # If dates are still missing, try to extract from beschreibung
        # The beschreibung might contain the full text including the date line
        if (not startdatum or not enddatum) and beschreibung:
            extracted_start, extracted_end = extract_dates_from_text(beschreibung)
            if extracted_start and not startdatum:
                startdatum = extracted_start
            if extracted_end and not enddatum:
                enddatum = extracted_end
        
        # Also check the full text content one more time if dates are still missing
        if (not startdatum or not enddatum):
            # Re-scan all text content we've seen
            current = heading.find_next_sibling("div")
            full_text = ""
            while current:
                if hasattr(current, "get_text"):
                    full_text += " " + current.get_text()
                if hasattr(current, "name") and current.name == "h2":
                    break
                current = current.find_next_sibling()
            
            if full_text:
                extracted_start, extracted_end = extract_dates_from_text(full_text)
                if extracted_start and not startdatum:
                    startdatum = extracted_start
                if extracted_end and not enddatum:
                    enddatum = extracted_end
        
        entry["startdatum"] = startdatum
        entry["enddatum"] = enddatum
        entry["beschreibung"] = beschreibung
        
        if entry["name_vernehmlassung"]:
            vernehmlassungen.append(entry)
    
    return vernehmlassungen


def extract_documents_from_page(soup: BeautifulSoup, page_url: str, vernehmlassung_name: str) -> list[dict]:
    """Extract document links from a Vernehmlassung page."""
    documents = []
    
    # Find all links that look like documents
    # Look for links in ul elements with class containing "grid" (document lists)
    link_lists = soup.find_all("ul", class_=lambda x: x and "grid" in x)
    
    for link_list in link_lists:
        links = link_list.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            
            # Remove "(Startet einen Download)" and similar text
            text = re.sub(r'\s*\([^)]*\)\s*', '', text).strip()
            
            # Check if it's a document link (PDF, DOC, or media.bs.ch links)
            is_document = (
                any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xlsx", ".xls"]) or
                "media.bs.ch" in href or
                "original_file" in href
            )
            
            if is_document:
                # Make absolute URL
                doc_url = urljoin(BASE_URL, href) if not href.startswith("http") else href
                
                # Determine file type from extension or URL
                file_ext = Path(urlparse(href).path).suffix.lower()
                if not file_ext and ".pdf" in href.lower():
                    file_ext = ".pdf"
                
                typ = "PDF" if file_ext == ".pdf" else "DOC" if file_ext in [".doc", ".docx"] else "XLS" if file_ext in [".xls", ".xlsx"] else "PDF"  # Default to PDF
                
                # Get filename from URL
                filename = os.path.basename(urlparse(href).path)
                if not filename or filename == "/":
                    # Try to extract from URL or use text
                    if "original_file" in href:
                        # Extract from URL like: .../original_file/hash/filename.pdf
                        parts = href.split("/")
                        if len(parts) > 0:
                            filename = parts[-1]
                    if not filename or filename == "/":
                        filename = text if text else f"document_{len(documents)}"
                        if not any(filename.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx"]):
                            filename += file_ext if file_ext else ".pdf"
                
                # Clean filename
                filename = filename.split("?")[0]  # Remove query parameters
                
                documents.append({
                    "Name": text if text else filename.replace(".pdf", "").replace("_", " "),
                    "Typ": typ,
                    "Dateiname": filename,
                    "Vernehmlassung": vernehmlassung_name,
                    "URL": doc_url,
                })
    
    return documents


def download_document(url: str, save_path: str):
    """Download a document from URL."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=60, stream=True)
    response.raise_for_status()
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def scrape_vernehmlassungen() -> pd.DataFrame:
    """Scrape all Vernehmlassungen from the 4 pages."""
    all_vernehmlassungen = []
    
    for page_url in VERNEHMLASSUNGEN_PAGES:
        logging.info(f"Scraping page: {page_url}")
        try:
            soup = fetch_page(page_url)
            vernehmlassungen = extract_vernehmlassungen_from_page(soup, page_url)
            all_vernehmlassungen.extend(vernehmlassungen)
            logging.info(f"Found {len(vernehmlassungen)} Vernehmlassungen on {page_url}")
        except Exception as e:
            logging.error(f"Error scraping {page_url}: {e}")
    
    df = pd.DataFrame(all_vernehmlassungen)
    
    # Convert dates to yyyy-mm-dd format (including those extracted from beschreibung)
    if "startdatum" in df.columns:
        df["startdatum"] = df["startdatum"].apply(parse_german_date)
    if "enddatum" in df.columns:
        df["enddatum"] = df["enddatum"].apply(parse_german_date)
    
    # Also try to extract dates from beschreibung if they're still missing
    for idx, row in df.iterrows():
        if (not row.get("startdatum") or not row.get("enddatum")) and row.get("beschreibung"):
            extracted_start, extracted_end = extract_dates_from_text(str(row.get("beschreibung", "")))
            if extracted_start and not row.get("startdatum"):
                df.at[idx, "startdatum"] = extracted_start
            if extracted_end and not row.get("enddatum"):
                df.at[idx, "enddatum"] = extracted_end
    
    return df


def scrape_and_process_documents() -> pd.DataFrame:
    """Scrape documents from Vernehmlassungen pages and process them."""
    all_documents = []
    
    # Load existing documents from Excel
    excel_path = os.path.join(DOKUMENTE_PATH, "Vorschlage_Dokumente_Vernehmlassungen.xlsx")
    existing_docs = []
    if os.path.exists(excel_path):
        existing_docs_df = pd.read_excel(excel_path)
        existing_docs = existing_docs_df.to_dict("records")
        logging.info(f"Loaded {len(existing_docs)} existing documents from Excel")
    
    # Get existing filenames to avoid duplicates
    existing_filenames = {str(doc.get("Dateiname", "")) for doc in existing_docs if "Dateiname" in doc}
    
    # Track filenames we've seen in this run to handle duplicates
    seen_filenames = set(existing_filenames)
    
    # Scrape documents directly from the Vernehmlassungen list pages
    # Documents are listed on the same pages as the Vernehmlassungen
    for page_url in VERNEHMLASSUNGEN_PAGES:
        logging.info(f"Scraping documents from: {page_url}")
        try:
            soup = fetch_page(page_url)
            
            # Find all h2 headings (Vernehmlassung names)
            headings = soup.find_all("h2", class_=lambda x: x and "container" in x and "paragraph--margin" in x)
            
            for heading in headings:
                vernehmlassung_name = heading.get_text(strip=True)
                if not vernehmlassung_name:
                    continue
                
                # Find documents in the section following this heading
                current = heading.find_next_sibling()
                while current:
                    if hasattr(current, "name") and current.name == "h2":
                        break
                    
                    # Look for link lists with documents
                    if hasattr(current, "find_all"):
                        link_lists = current.find_all("ul", class_=lambda x: x and "grid" in x)
                        for link_list in link_lists:
                            links = link_list.find_all("a", href=True)
                            for link in links:
                                href = link.get("href", "")
                                text = link.get_text(strip=True)
                                
                                # Remove "(Startet einen Download)" and similar text
                                text = re.sub(r'\s*\([^)]*\)\s*', '', text).strip()
                                
                                # Check if it's a document link
                                is_document = (
                                    any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xlsx", ".xls"]) or
                                    "media.bs.ch" in href or
                                    "original_file" in href
                                )
                                
                                if is_document:
                                    # Make absolute URL
                                    doc_url = urljoin(BASE_URL, href) if not href.startswith("http") else href
                                    
                                    # Determine file type
                                    file_ext = Path(urlparse(href).path).suffix.lower()
                                    if not file_ext and ".pdf" in href.lower():
                                        file_ext = ".pdf"
                                    
                                    typ = "PDF" if file_ext == ".pdf" else "DOC" if file_ext in [".doc", ".docx"] else "XLS" if file_ext in [".xls", ".xlsx"] else "PDF"
                                    
                                    # Get original filename
                                    filename_orig = os.path.basename(urlparse(href).path)
                                    if not filename_orig or filename_orig == "/":
                                        if "original_file" in href:
                                            parts = href.split("/")
                                            if len(parts) > 0:
                                                filename_orig = parts[-1]
                                        if not filename_orig or filename_orig == "/":
                                            filename_orig = text if text else f"document_{len(all_documents)}"
                                            if not any(filename_orig.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx"]):
                                                filename_orig += file_ext if file_ext else ".pdf"
                                    
                                    filename_orig = filename_orig.split("?")[0]  # Remove query parameters
                                    
                                    # Handle duplicate filenames by adding a counter
                                    filename_final = filename_orig
                                    counter = 1
                                    while filename_final in seen_filenames:
                                        # Add counter before extension
                                        name_part = Path(filename_final).stem
                                        ext_part = Path(filename_final).suffix
                                        filename_final = f"{name_part}_{counter}{ext_part}"
                                        counter += 1
                                    # Reserve this filename so duplicates get a different suffix (also when doc is in Excel and we re-download)
                                    seen_filenames.add(filename_final)
                                    
                                    # Check if we already have this document in Excel
                                    doc_in_excel = any(
                                        str(doc.get("Dateiname", "")) == filename_orig or 
                                        (doc.get("URL") and str(doc.get("URL", "")) == doc_url)
                                        for doc in existing_docs
                                    )
                                    
                                    # Check if file exists on disk
                                    save_path_orig = os.path.join(DOKUMENTE_PATH, filename_final)
                                    file_exists = os.path.exists(save_path_orig)
                                    
                                    # If document is new, add it to list
                                    if not doc_in_excel:
                                        doc = {
                                            "Name": text if text else filename_orig.replace(".pdf", "").replace("_", " "),
                                            "Typ": typ,
                                            "Dateiname": filename_final,  # Use final filename (with counter if needed)
                                            "Vernehmlassung": vernehmlassung_name,
                                            "URL": doc_url,
                                        }
                                        all_documents.append(doc)
                                    
                                    # Download the document if it doesn't exist (whether new or existing in Excel)
                                    if not file_exists:
                                        try:
                                            download_document(doc_url, save_path_orig)
                                            logging.info(f"Downloaded missing file: {filename_final}")
                                        except Exception as e:
                                            logging.error(f"Error downloading {doc_url}: {e}")
                                            if not doc_in_excel:
                                                # Remove from list if download failed for new document
                                                if 'doc' in locals():
                                                    if doc in all_documents:
                                                        all_documents.remove(doc)
                                                        seen_filenames.discard(filename_final)
                    
                    current = current.find_next_sibling()
                    
        except Exception as e:
            logging.error(f"Error processing page {page_url}: {e}")
    
    # Include all existing docs (they will be filtered later if files don't exist)
    # The files should have been downloaded during scraping if they were missing
    all_documents = existing_docs + all_documents
    
    # Remove duplicates based on Dateiname
    seen_dates = set()
    unique_documents = []
    for doc in all_documents:
        dateiname = str(doc.get("Dateiname", ""))
        if dateiname and dateiname not in seen_dates:
            seen_dates.add(dateiname)
            unique_documents.append(doc)
    all_documents = unique_documents
    
    # Update Excel file (without URL column)
    if all_documents:
        df = pd.DataFrame(all_documents)
        # Remove URL column before saving to Excel
        if "URL" in df.columns:
            df_excel = df.drop(columns=["URL"])
        else:
            df_excel = df
        df_excel.to_excel(excel_path, index=False)
        logging.info(f"Updated Excel file with {len(all_documents)} documents")
    
    return pd.DataFrame(all_documents) if all_documents else pd.DataFrame(columns=["Name", "Typ", "Dateiname", "Vernehmlassung"])


def process_documents_for_ftp(df: pd.DataFrame) -> pd.DataFrame:
    """Process documents for FTP upload: sanitize filenames, copy to data/dokumente, upload to FTP."""
    df = df.copy()
    df["Dateiname"] = df["Dateiname"].astype(str)
    
    # Create data/dokumente directory
    data_dokumente_path = os.path.join("data", "dokumente")
    os.makedirs(data_dokumente_path, exist_ok=True)
    
    # Sanitize filename for FTP
    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)
    
    # Ensure PDFs keep/get the .pdf suffix
    def ensure_pdf_suffix(orig_name: str, ftp_name: str) -> str:
        orig_path = Path(orig_name)
        ftp_path = Path(ftp_name)
        if orig_path.suffix.lower() == ".pdf" and ftp_path.suffix.lower() != ".pdf":
            return str(ftp_path.with_suffix(".pdf"))
        return ftp_name
    
    df["Dateiname_ftp"] = [ensure_pdf_suffix(o, f) for o, f in zip(df["Dateiname"], df["Dateiname_ftp"])]
    
    # Handle duplicate sanitized filenames
    seen_ftp_names = {}
    ftp_names_final = []
    for ftp_name in df["Dateiname_ftp"]:
        if ftp_name in seen_ftp_names:
            seen_ftp_names[ftp_name] += 1
            counter = seen_ftp_names[ftp_name]
            name_part = Path(ftp_name).stem
            ext_part = Path(ftp_name).suffix
            ftp_name_final = f"{name_part}_{counter}{ext_part}"
        else:
            seen_ftp_names[ftp_name] = 0
            ftp_name_final = ftp_name
        ftp_names_final.append(ftp_name_final)
    
    df["Dateiname_ftp"] = ftp_names_final
    
    # Create FTP URL
    base_url = "https://data-bs.ch/stata/staka/vernehmlassungen/dokumente/"
    df["URL_Datei"] = base_url + df["Dateiname_ftp"]
    
    return df


def upload_documents_to_ftp(df: pd.DataFrame):
    """Upload documents to FTP server."""
    remote_dir = "staka/vernehmlassungen/"
    remote_dir_dokumente = "staka/vernehmlassungen/dokumente/"
    data_dokumente_path = os.path.join("data", "dokumente")
    os.makedirs(data_dokumente_path, exist_ok=True)
    
    for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"]):
        # Source: original file in data_orig/Dokumente
        # The orig_name might have _1, _2 suffixes from duplicate handling, or might not
        src_path = os.path.join(DOKUMENTE_PATH, orig_name)
        
        # If file doesn't exist with exact name, try to find it with _1, _2, etc. suffixes
        if not os.path.exists(src_path):
            # Try to find file with suffix
            name_part = Path(orig_name).stem
            ext_part = Path(orig_name).suffix
            found = False
            for suffix in ["", "_1", "_2", "_3", "_4", "_5"]:
                try_name = f"{name_part}{suffix}{ext_part}"
                try_path = os.path.join(DOKUMENTE_PATH, try_name)
                if os.path.exists(try_path):
                    src_path = try_path
                    found = True
                    logging.info(f"Found file with suffix: {try_name} (looking for {orig_name})")
                    break
            
            if not found:
                logging.warning(f"File not found: {orig_name} (tried with suffixes), skipping")
                continue
        
        # Destination: sanitized file in data/dokumente
        dst_path = os.path.join(data_dokumente_path, ftp_name)
        shutil.copy2(src_path, dst_path)
        logging.info(f"Copied {os.path.basename(src_path)} to {dst_path}")
        
        # Upload to FTP
        common.upload_ftp(dst_path, remote_path=remote_dir_dokumente)
        logging.info(f"Uploaded {os.path.basename(src_path)} as {ftp_name} to FTP at {remote_dir_dokumente}")
    
    # Save CSV with URL_Datei column
    csv_filename = "100515_dokumente_vernehmlassungen.csv"
    csv_file_path = os.path.join("data", csv_filename)
    df_out = df.drop(columns=["Dateiname_ftp"])
    df_out.to_csv(csv_file_path, index=False)
    common.update_ftp_and_odsp(csv_file_path, remote_dir, dataset_id="100515")
    logging.info(f"Saved and uploaded {csv_filename}")


def process_textrueckmeldungen():
    """Concatenate all Excel files in Textrueckmeldungen folder."""
    if not os.path.exists(TEXTRUECKMELDUNGEN_PATH):
        logging.warning(f"Textrueckmeldungen folder not found: {TEXTRUECKMELDUNGEN_PATH}")
        return
    
    all_dataframes = []
    
    for filename in os.listdir(TEXTRUECKMELDUNGEN_PATH):
        if filename.endswith((".xlsx", ".xls")):
            file_path = os.path.join(TEXTRUECKMELDUNGEN_PATH, filename)
            try:
                df = pd.read_excel(file_path)
                all_dataframes.append(df)
                logging.info(f"Loaded {filename} with {len(df)} rows")
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")
    
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        csv_filename = "100514_textrueckmeldungen_vernehmlassungen.csv"
        csv_file_path = os.path.join("data", csv_filename)
        os.makedirs("data", exist_ok=True)
        combined_df.to_csv(csv_file_path, index=False)
        logging.info(f"Saved combined Textrueckmeldungen to {csv_file_path}")
        
        # Upload to FTP
        remote_dir = "staka/vernehmlassungen/"
        common.update_ftp_and_odsp(csv_file_path, remote_dir, dataset_id="100514")
        logging.info(f"Uploaded {csv_filename} to FTP")
    else:
        logging.warning("No Excel files found in Textrueckmeldungen folder")


def main():
    """Main ETL function."""
    logging.info("ETL job started")
    
    # Ensure directories exist
    os.makedirs(DOKUMENTE_PATH, exist_ok=True)
    os.makedirs(TEXTRUECKMELDUNGEN_PATH, exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    # 1. Scrape Vernehmlassungen
    logging.info("Scraping Vernehmlassungen...")
    vernehmlassungen_df = scrape_vernehmlassungen()
    
    if not vernehmlassungen_df.empty:
        csv_path = os.path.join("data", "100516_vernehmlassung.csv")
        vernehmlassungen_df.to_csv(csv_path, index=False)
        logging.info(f"Saved {len(vernehmlassungen_df)} Vernehmlassungen to {csv_path}")
        
        # Upload to FTP
        remote_dir = "staka/vernehmlassungen/"
        common.update_ftp_and_odsp(csv_path, remote_dir, dataset_id="100516")
        logging.info(f"Uploaded 100516_vernehmlassung.csv to FTP")
    else:
        logging.warning("No Vernehmlassungen found")
    
    # 2. Scrape and process documents
    logging.info("Scraping and processing documents...")
    documents_df = scrape_and_process_documents()
    
    if not documents_df.empty:
        documents_df = process_documents_for_ftp(documents_df)
        upload_documents_to_ftp(documents_df)
    else:
        logging.warning("No documents found")
    
    # 3. Process Textrueckmeldungen
    logging.info("Processing Textrueckmeldungen...")
    process_textrueckmeldungen()
    
    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
