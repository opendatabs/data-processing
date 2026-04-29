"""Extract raw Dataspot metadata into source files."""

import json
import logging
from html import escape

from dataspot_auth import DataspotAuth
import common
from paths import DATA_ORIG_DIR


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DATENKATALOG_URL = (
    "https://datenkatalog.bs.ch/api/prod/schemes/"
    "Datenprodukte/download?format=json&resourceTypes=Dataset"
)
COMPOSITIONS_URL_TEMPLATE = "https://bs.dataspot.io/rest/prod/datasets/{dataset_id}/compositions"

OUTPUT_DIR = DATA_ORIG_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUTPUT_FILE = OUTPUT_DIR / "geo_datasets.json"
HTML_OUTPUT_FILE = OUTPUT_DIR / "geo_datsets.html"

auth = DataspotAuth()


def fetch_datasets() -> list[dict]:
    headers = auth.get_headers()
    response = common.requests_get(url=DATENKATALOG_URL, headers=headers)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Die Antwort vom Datenkatalog ist keine Liste.")

    return data


def fetch_compositions(dataset_id: str) -> list[dict]:
    headers = auth.get_headers()
    url = COMPOSITIONS_URL_TEMPLATE.format(dataset_id=dataset_id)
    response = common.requests_get(url=url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("_embedded", {}).get("compositions", [])


def is_geo_dataset(dataset: dict) -> bool:
    stereotype = str(dataset.get("stereotype", "")).strip().upper()
    return "GEO" in stereotype


def is_geopaket(dataset: dict) -> bool:
    stereotype = str(dataset.get("stereotype", "")).strip().upper()
    return "PAKET" in stereotype


def is_dataset_composition(composition: dict) -> bool:
    href = composition.get("_links", {}).get("composedOf", {}).get("href", "")
    return "/rest/prod/datasets/" in href


def build_dataset_lookup(datasets):
    lookup = {}
    for dataset in datasets:
        dataset_id = dataset.get("id")
        if dataset_id:
            lookup[str(dataset_id).strip()] = dataset
    return lookup


def build_child_from_dataset(dataset):
    return {
        "label": dataset.get("label"),
        "id": dataset.get("id"),
        "productLayername": dataset.get("productLayername"),
        "inCollection": dataset.get("inCollection")
    }


def build_child_from_composition(composition, dataset_lookup):
    child_id = composition.get("composedOf")
    child_id = str(child_id).strip() if child_id else None
    child_dataset = dataset_lookup.get(child_id, {})

    return {
        "label": child_dataset.get("label") or composition.get("label"),
        "id": child_id,
        "productLayername": child_dataset.get("productLayername"),
        "inCollection": child_dataset.get("inCollection")
    }


def build_hierarchy(datasets):
    geo_datasets = [dataset for dataset in datasets if is_geo_dataset(dataset)]
    dataset_lookup = build_dataset_lookup(geo_datasets)

    paket_geo = []
    single_geo_candidates = []
    child_ids = set()

    for dataset in geo_datasets:
        dataset_id = dataset.get("id")
        if not dataset_id:
            continue

        dataset_id = str(dataset_id).strip()

        if is_geopaket(dataset):
            try:
                compositions = fetch_compositions(dataset_id)
            except Exception as e:
                logging.warning("Compositions konnten nicht geladen werden für %s: %s", dataset_id, e)
                compositions = []

            children = []
            for composition in compositions:
                if not is_dataset_composition(composition):
                    continue

                child = build_child_from_composition(composition, dataset_lookup)
                if child["id"]:
                    child_ids.add(str(child["id"]).strip())
                    children.append(child)

            paket_geo.append({
                "type": "paket_geo",
                "title": dataset.get("label"),
                "children": children
            })
        else:
            single_geo_candidates.append(dataset)

    single_geo = []
    for dataset in single_geo_candidates:
        dataset_id = str(dataset.get("id", "")).strip()
        if not dataset_id:
            continue

        if dataset_id in child_ids:
            continue

        single_geo.append({
            "type": "single_geo",
            "title": dataset.get("label"),
            "children": [
                build_child_from_dataset(dataset)
            ]
        })

    hierarchy = paket_geo + single_geo
    hierarchy.sort(key=lambda x: str(x.get("title", "")).lower())

    return hierarchy


def save_json(hierarchy):
    with open(JSON_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(hierarchy, f, ensure_ascii=False, indent=2)


def html_table_from_children(children):
    if not children:
        return '<p class="empty-state">Keine Children gefunden.</p>'

    rows = []
    for child in children:
        label = escape("" if child.get("label") is None else str(child.get("label")))
        child_id = escape("" if child.get("id") is None else str(child.get("id")))
        layer = escape("" if child.get("productLayername") is None else str(child.get("productLayername")))
        in_collection = escape("" if child.get("inCollection") is None else str(child.get("inCollection")))
        rows.append(
            f"""
            <tr>
              <td>{label}</td>
              <td class="mono">{child_id}</td>
              <td>{layer}</td>
              <td class="mono">{in_collection}</td>
            </tr>
            """
        )

    return f"""
    <div class="table-shell">
      <table class="data-table">
        <thead>
          <tr>
            <th>Label</th>
            <th>ID</th>
            <th>ProductLayername</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </div>
    """


def build_html(hierarchy):
    sections = []

    for record in hierarchy:
        record_type = escape(str(record.get("type", "")))
        title = escape(str(record.get("title", "")))
        children = record.get("children", [])
        count = len(children)

        sections.append(
            f"""
            <details class="content-card">
              <summary class="content-card-summary">
                <div class="content-card-main">
                  <div class="eyebrow">{record_type}</div>
                  <h2>{title}</h2>
                </div>
                <div class="content-card-side">
                  <span class="count-pill">{count} Datensätze</span>
                  <span class="chevron" aria-hidden="true">▾</span>
                </div>
              </summary>
              <div class="content-card-body">
                {html_table_from_children(children)}
              </div>
            </details>
            """
        )

    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Geo-Daten</title>
  <style>
    :root {{
      --page-bg: #ffffff;
      --surface: #ffffff;
      --surface-soft: #f3f4f4;
      --surface-muted: #f7f8f7;
      --border: #cfd4d6;
      --border-strong: #aeb8bc;
      --text: #12384c;
      --text-soft: #50616d;
      --primary: #005ea5;
      --primary-soft: #e8f1f8;
      --accent-red: #c8102e;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
      --radius-lg: 18px;
      --radius-md: 12px;
      --container: 1180px;
    }}

    * {{
      box-sizing: border-box;
    }}

    html {{
      scroll-behavior: smooth;
    }}

    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: var(--text);
      background: var(--page-bg);
      line-height: 1.5;
    }}

    .site-header {{
      position: sticky;
      top: 0;
      z-index: 30;
      background: #fff;
      border-bottom: 1px solid var(--border-strong);
    }}

    .site-header-inner,
    .container {{
      width: min(var(--container), calc(100% - 32px));
      margin: 0 auto;
    }}

    .site-header-inner {{
      display: flex;
      align-items: center;
      gap: 14px;
      min-height: 78px;
    }}

    .brand {{
      display: inline-flex;
      align-items: center;
      gap: 12px;
      text-decoration: none;
      color: var(--text);
      font-weight: 700;
      font-size: 22px;
      line-height: 1;
    }}

    .brand-mark {{
      width: 14px;
      height: 40px;
      background: var(--accent-red);
      border-radius: 3px;
      display: inline-block;
      flex: 0 0 auto;
    }}

    .hero {{
      background: #fff;
      border-bottom: 1px solid var(--border);
    }}

    .hero-inner {{
      width: min(var(--container), calc(100% - 32px));
      margin: 0 auto;
      padding: 32px 0 26px;
    }}

    .breadcrumb {{
      color: var(--text-soft);
      font-size: 14px;
      margin-bottom: 18px;
    }}

    h1 {{
      margin: 0 0 10px;
      color: var(--primary);
      font-size: 42px;
      line-height: 1.12;
      letter-spacing: -0.01em;
    }}

    .lead {{
      margin: 0;
      max-width: 900px;
      color: var(--text-soft);
      font-size: 20px;
    }}

    .content {{
      background: var(--surface-muted);
      padding: 30px 0 48px;
    }}

    .section-heading {{
      margin: 0 0 20px;
      color: var(--primary);
      font-size: 18px;
      font-weight: 700;
    }}

    .content-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      overflow: hidden;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }}

    .content-card-summary {{
      list-style: none;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 20px;
      padding: 22px 24px;
      background: #fff;
      transition: background 0.15s ease;
    }}

    .content-card-summary:hover {{
      background: var(--surface-soft);
    }}

    .content-card-summary::-webkit-details-marker {{
      display: none;
    }}

    .content-card-main {{
      min-width: 0;
    }}

    .eyebrow {{
      display: inline-block;
      margin-bottom: 10px;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--primary-soft);
      color: var(--primary);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}

    .content-card h2 {{
      margin: 0;
      font-size: 28px;
      line-height: 1.2;
      color: var(--text);
    }}

    .content-card-side {{
      display: flex;
      align-items: center;
      gap: 14px;
      flex: 0 0 auto;
    }}

    .count-pill {{
      display: inline-flex;
      align-items: center;
      min-height: 32px;
      padding: 0 12px;
      border-radius: 999px;
      border: 1px solid var(--border);
      color: var(--text-soft);
      font-size: 14px;
      background: #fff;
      white-space: nowrap;
    }}

    .chevron {{
      color: var(--primary);
      font-size: 22px;
      line-height: 1;
      transition: transform 0.18s ease;
    }}

    .content-card[open] .chevron {{
      transform: rotate(180deg);
    }}

    .content-card[open] .content-card-summary {{
      border-bottom: 1px solid var(--border);
      background: #fbfcfc;
    }}

    .content-card-body {{
      padding: 20px 24px 24px;
      background: #fff;
    }}

    .table-shell {{
      overflow-x: auto;
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      background: #fff;
    }}

    .data-table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
      font-size: 15px;
    }}

    .data-table thead th {{
      text-align: left;
      padding: 14px 16px;
      background: #f1f4f5;
      color: var(--text);
      border-bottom: 1px solid var(--border);
      font-weight: 700;
    }}

    .data-table tbody td {{
      padding: 14px 16px;
      border-bottom: 1px solid #e7ebec;
      vertical-align: top;
      color: #1a2f3c;
    }}

    .data-table tbody tr:last-child td {{
      border-bottom: none;
    }}

    .data-table tbody tr:hover {{
      background: #fafcfc;
    }}

    .mono {{
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 13px;
      color: #355164;
      word-break: break-word;
    }}

    .empty-state {{
      margin: 0;
      color: var(--text-soft);
      font-style: italic;
    }}

    .site-footer {{
      border-top: 4px solid #2e7d32;
      background: #eceeed;
      padding: 24px 0 30px;
    }}

    .site-footer p {{
      margin: 0;
      color: var(--text-soft);
      font-size: 14px;
    }}

    @media (max-width: 860px) {{
      .site-header-inner {{
        min-height: 66px;
      }}

      .brand {{
        font-size: 19px;
      }}

      h1 {{
        font-size: 34px;
      }}

      .lead {{
        font-size: 18px;
      }}

      .content-card-summary {{
        flex-direction: column;
        align-items: flex-start;
      }}

      .content-card-side {{
        width: 100%;
        justify-content: space-between;
      }}

      .content-card h2 {{
        font-size: 24px;
      }}
    }}

    @media (max-width: 560px) {{
      .hero-inner {{
        padding: 24px 0 22px;
      }}

      h1 {{
        font-size: 28px;
      }}

      .lead {{
        font-size: 16px;
      }}

      .content {{
        padding-top: 22px;
      }}
    }}
  </style>
</head>
<body>
  <header class="site-header">
    <div class="site-header-inner">
      <a class="brand" href="#" aria-label="Kanton Basel-Stadt">
        <span class="brand-mark" aria-hidden="true"></span>
        <span>Kanton Basel-Stadt</span>
      </a>
    </div>
  </header>

  <section class="hero">
    <div class="hero-inner">
      <div class="breadcrumb">Daten &nbsp;/&nbsp; Geo-Daten</div>
      <h1>Geo-Daten</h1>
      <p class="lead">Klicken Sie auf einen Haupttitel, um die zugehörigen Geodatensätze anzuzeigen.</p>
    </div>
  </section>

  <main class="content">
    <div class="container">
      <h2 class="section-heading">Übersicht</h2>
      {''.join(sections)}
    </div>
  </main>

  <footer class="site-footer">
    <div class="container">
      <p>Generierte Übersicht der Geo-Datensätze.</p>
    </div>
  </footer>
</body>
</html>
"""


def save_html(hierarchy):
    html = build_html(hierarchy)
    with open(HTML_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    logging.info("Datasets laden ...")
    datasets = fetch_datasets()

    logging.info("Hierarchie bauen ...")
    hierarchy = build_hierarchy(datasets)

    logging.info("JSON speichern ...")
    save_json(hierarchy)

    logging.info("HTML speichern ...")
    save_html(hierarchy)

    logging.info("Fertig:")
    logging.info("JSON: %s", JSON_OUTPUT_FILE)
    logging.info("HTML: %s", HTML_OUTPUT_FILE)


if __name__ == "__main__":
    main()