"""Publish changed StatRhena files and rebuild the HTML viewers.

Files in ``data_orig`` are uploaded to the FTP folder ``statrhena`` and the
matching Huwise/ODS dataset is republished when the file content changes.
``data/index.html`` and the language-specific pages are rebuilt from those
files on every run.
"""

from __future__ import annotations

import base64
import csv
import gzip
import io
import json
import logging
import mimetypes
import os
import pathlib
import re

import common
from dotenv import load_dotenv

load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parent
DATA_ORIG = ROOT / "data_orig"
OUT_DIR = ROOT / "data"
ASSETS = ROOT / "assets"
TOPIC_BEVOELKERUNG = "1_Bevölkerung"
TOPIC_WIRTSCHAFT = "2_Wirtschaft_Arbeit"
COUNTRIES = (("ch", "CH"), ("de", "DE"), ("fr", "FR"))
FTP_REMOTE_PATH = "statrhena"
_DATASET_FILE = re.compile(r"^(\d+)_.*\.csv$")

LANG_SWITCHER_HTML = """\
  <div class="lang" id="langSwitch">
    <button type="button" class="lang-btn" id="langBtn" aria-haspopup="listbox" aria-expanded="false">
      <img id="langFlag" alt="" />
      <span id="langLabel">Deutsch</span>
      <span class="caret">▾</span>
    </button>
    <div class="lang-menu" role="listbox" id="langMenu"></div>
  </div>"""


def b64(path: pathlib.Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def data_uri(path: pathlib.Path) -> str:
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    if path.suffix == ".svg":
        mime = "image/svg+xml"
    return f"data:{mime};base64,{b64(path)}"


def nav_icon(filename: str) -> str:
    raw = (ASSETS / filename).read_text(encoding="utf-8")
    match = re.search(r"<svg[^>]*>(.*)</svg>", raw, flags=re.DOTALL)
    body = match.group(1).strip() if match else ""
    return f'<svg viewBox="0 0 24 24" aria-hidden="true">{body}</svg>'


def render_html(font_css: str, payload: dict, *, fixed_lang: str | None, show_lang: bool) -> str:
    html_lang = fixed_lang or "de"
    return (
        TEMPLATE.replace("__FONT_CSS__", font_css)
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
        .replace("__HTML_LANG__", html_lang)
        .replace("__FIXED_LANG__", json.dumps(fixed_lang))
        .replace("__SHOW_LANG__", "true" if show_lang else "false")
        .replace("__LANG_SWITCHER__", LANG_SWITCHER_HTML if show_lang else "")
        .replace("__ICON_BEVOELKERUNG__", nav_icon("users.svg"))
        .replace("__ICON_WIRTSCHAFT__", nav_icon("settings.svg"))
        .replace("__ICON_GESELLSCHAFT__", nav_icon("message-square-more.svg"))
        .replace("__ICON_UMWELT__", nav_icon("rotate-ccw.svg"))
    )


def _text(value: str | None) -> str:
    text = (value or "").strip()
    return "" if text == "NA" else text


def _country_value(row: dict[str, str], field: str, prefix: str) -> str:
    primary = _text(row.get(f"{field}_{prefix}1"))
    secondary = _text(row.get(f"{field}_{prefix}2"))
    if primary and secondary and primary != secondary:
        return f"{primary} · {secondary}"
    return primary or secondary


def _timespan(row: dict[str, str], prefix: str) -> list[str]:
    start = _text(row.get(f"Timespan_Min_{prefix}1"))
    end = _text(row.get(f"Timespan_Max_{prefix}1"))
    if not start and not end:
        start = _text(row.get(f"Timespan_Min_{prefix}2"))
        end = _text(row.get(f"Timespan_Max_{prefix}2"))
    return [start, end]


def _read_topic(path: pathlib.Path, topic: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [row for row in csv.DictReader(handle) if _text(row.get("Topic")) == topic]


def _metadata_for(topic: str) -> dict:
    """Shape one topic from the metadata CSVs into the structure the page reads."""
    short_rows = _read_topic(DATA_ORIG / "100554_statrhena_metadaten_1_databs.csv", topic)
    long_rows = _read_topic(DATA_ORIG / "100555_statrhena_metadaten_2_databs.csv", topic)
    subtopics: list[str] = []
    for row in short_rows:
        subtopic = _text(row.get("Subtopic"))
        if subtopic and subtopic not in subtopics:
            subtopics.append(subtopic)
    short = [
        {
            "subtopic": _text(row.get("Subtopic")),
            "title": _text(row.get("Title")),
            "variable": _text(row.get("Variable")),
            "description": _text(row.get("Variable_Description")),
            "unit": _text(row.get("Measure_Unit")),
            "min": _text(row.get("Timespan_Min")),
            "max": _text(row.get("Timespan_Max")),
            "source": _text(row.get("Source")),
            "footnote": _text(row.get("Footnote")),
        }
        for row in short_rows
    ]
    long = [
        {
            "variable": _text(row.get("Variable")),
            "title": _text(row.get("Title")),
            "description": _text(row.get("Variable_Description")),
            "footnote": _text(row.get("Footnote")),
            "titles": {code: _country_value(row, "Title", prefix) for code, prefix in COUNTRIES},
            "stats": {code: _country_value(row, "Statistic", prefix) for code, prefix in COUNTRIES},
            "info": {code: _country_value(row, "Specific_Information", prefix) for code, prefix in COUNTRIES},
            "sources": {code: _country_value(row, "Source", prefix) for code, prefix in COUNTRIES},
            "timespan": {code: _timespan(row, prefix) for code, prefix in COUNTRIES},
        }
        for row in long_rows
    ]
    return {"short": short, "long": long, "subtopics": subtopics}


def build_metadata() -> dict:
    return {
        TOPIC_BEVOELKERUNG: _metadata_for(TOPIC_BEVOELKERUNG),
        TOPIC_WIRTSCHAFT: _metadata_for(TOPIC_WIRTSCHAFT),
    }


def gzip_b64(data: bytes) -> str:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as handle:
        handle.write(data)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_pages() -> None:
    """Write the self-contained viewers into data/ from the current data_orig files."""
    fonts = json.loads((ASSETS / "fonts-b64.json").read_text())
    font_css = "\n".join(
        f"""@font-face {{
  font-family: 'Work Sans';
  font-style: normal;
  font-weight: {w};
  font-display: swap;
  src: url(data:font/woff2;base64,{fonts[w]}) format('woff2');
}}"""
        for w in ("400", "500", "600", "700")
        if w in fonts
    )

    payload = {
        "bevoelkerung_gz": gzip_b64((DATA_ORIG / "100550_statrhena_bevoelkerung_databs.csv").read_bytes()),
        "wirtschaft_gz": gzip_b64((DATA_ORIG / "100551_statrhena_wirtschaft_arbeit_databs.csv").read_bytes()),
        "metadata_gz": gzip_b64(json.dumps(build_metadata(), ensure_ascii=False).encode("utf-8")),
        "flags": {
            "de": data_uri(ASSETS / "de.svg"),
            "en": data_uri(ASSETS / "en.svg"),
            "fr": data_uri(ASSETS / "fr.svg"),
            "ch": data_uri(ASSETS / "ch.svg"),
        },
    }

    variants = [
        ("index.html", None, True),
        ("index_de.html", "de", False),
        ("index_en.html", "en", False),
        ("index_fr.html", "fr", False),
    ]
    OUT_DIR.mkdir(exist_ok=True)
    for name, fixed_lang, show_lang in variants:
        out = OUT_DIR / name
        out.write_text(
            render_html(font_css, payload, fixed_lang=fixed_lang, show_lang=show_lang),
            encoding="utf-8",
        )
        logging.info("Wrote %s (%s KB)", out.name, round(out.stat().st_size / 1024))


def publish_changed_files() -> None:
    """Upload each changed source CSV and republish its Huwise/ODS dataset."""
    for name in sorted(os.listdir(DATA_ORIG)):
        match = _DATASET_FILE.match(name)
        if not match:
            continue
        path = str(DATA_ORIG / name)
        dataset_id = match.group(1)
        logging.info("Checking %s (dataset %s)", name, dataset_id)
        common.update_ftp_and_odsp(path, FTP_REMOTE_PATH, dataset_id)


def main() -> None:
    logging.info("Building HTML viewers from data_orig")
    build_pages()
    publish_changed_files()


TEMPLATE = r"""<!DOCTYPE html>
<html lang="__HTML_LANG__">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>StatRhena: Daten &amp; Metadaten</title>
<style>
__FONT_CSS__

:root {
  --blue: #116a8d;
  --blue-dark: #0e5673;
  --ink: #222531;
  --muted: #5b6070;
  --line: #e6e7eb;
  --soft: #f7f7f7;
  --footer: #222531;
  --font: "Work Sans", system-ui, sans-serif;
}

*, *::before, *::after { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  font-family: var(--font);
  color: var(--ink);
  background: #fff;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}
a { color: inherit; text-decoration: none; }
img { max-width: 100%; display: block; }
button, select, input { font: inherit; }

.page-bar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px 24px;
  margin-bottom: 24px;
}
.page-bar .page-title { margin: 0; flex: 1; min-width: 0; }

.lang { position: relative; flex: 0 0 auto; margin-top: 6px; }
.lang-btn {
  display: inline-flex; align-items: center; gap: 8px;
  padding: 7px 12px;
  border: 1px solid var(--line);
  border-radius: 4px;
  background: #fff;
  cursor: pointer;
  color: var(--ink);
  font-weight: 500;
  font-size: .92rem;
}
.lang-btn img { width: 22px; height: 16px; object-fit: cover; border-radius: 2px; }
.lang-btn .caret { font-size: .7rem; color: var(--muted); }
.lang-menu {
  display: none;
  position: absolute; right: 0; top: calc(100% + 6px);
  min-width: 160px;
  background: #fff;
  border: 1px solid var(--line);
  box-shadow: 0 12px 28px rgba(34,37,49,.1);
  padding: 6px 0;
  z-index: 60;
}
.lang.open .lang-menu { display: block; }
.lang-menu button {
  width: 100%;
  display: flex; align-items: center; gap: 10px;
  padding: 9px 14px;
  border: 0; background: none;
  cursor: pointer;
  text-align: left;
  font-weight: 500;
}
.lang-menu button:hover, .lang-menu button.active { background: var(--soft); }
.lang-menu img { width: 22px; height: 16px; object-fit: cover; border-radius: 2px; }

.wrap {
  width: min(1600px, 100%);
  margin: 0 auto;
  padding: 40px 32px 48px;
}

.page-title {
  margin: 0 0 24px;
  font-size: clamp(2.1rem, 4vw, 2.85rem);
  font-weight: 700;
  letter-spacing: -0.025em;
}

.theme-nav {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 36px;
}
@media (max-width: 800px) {
  .theme-nav { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
.theme-nav a {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: 16px;
  min-height: 96px;
  padding: 16px;
  background: var(--blue);
  color: #fff;
  transition: background .15s ease;
}
.theme-nav a:hover { background: var(--blue-dark); }
.theme-nav a.soon { opacity: .72; }
.theme-nav svg {
  width: 30px; height: 30px;
  stroke: #fff; fill: none; stroke-width: 2;
  stroke-linecap: round; stroke-linejoin: round;
}
.theme-nav span { font-weight: 600; font-size: .98rem; }

.theme {
  padding: 40px 0 52px;
  border-bottom: 1px solid var(--line);
  scroll-margin-top: 24px;
}
.theme:last-of-type { border-bottom: 0; }
.theme h2 {
  margin: 0 0 10px;
  font-size: clamp(1.45rem, 2.4vw, 1.85rem);
  font-weight: 600;
  letter-spacing: -0.02em;
}
.theme-desc {
  margin: 0 0 22px;
  color: var(--muted);
  max-width: 72ch;
  font-size: 1.02rem;
}

.block-title {
  margin: 28px 0 12px;
  font-size: 1.05rem;
  font-weight: 600;
}
.block-title:first-of-type { margin-top: 0; }

/* Multiselect filters */
.filters {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px 16px;
  margin-bottom: 18px;
  align-items: end;
}
@media (max-width: 900px) {
  .filters { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 560px) {
  .filters { grid-template-columns: 1fr; }
}
.filter-field {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.filter-field > .flabel {
  font-size: .75rem;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: .03em;
  line-height: 1.25;
  overflow-wrap: anywhere;
  word-break: break-word;
}
.ms { position: relative; }
.ms-btn {
  width: 100%;
  display: flex; align-items: center; justify-content: space-between; gap: 8px;
  padding: 9px 10px;
  border: 1px solid var(--line);
  background: #fff;
  cursor: pointer;
  text-align: left;
  font-size: .9rem;
  color: var(--ink);
  min-height: 40px;
}
.ms-btn .ms-summary {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
  flex: 1;
  color: var(--muted);
}
.ms-btn .ms-summary.has { color: var(--ink); font-weight: 500; }
.ms-btn .ms-caret { color: var(--muted); flex: 0 0 auto; }
.ms.open .ms-btn { border-color: var(--blue); }
.ms-panel {
  display: none;
  position: absolute; left: 0; right: 0; top: calc(100% + 4px);
  max-height: 240px;
  overflow: auto;
  background: #fff;
  border: 1px solid var(--line);
  box-shadow: 0 10px 28px rgba(34,37,49,.12);
  z-index: 40;
  padding: 6px 0;
}
.ms.open .ms-panel { display: block; }
.ms-panel label {
  display: flex; align-items: flex-start; gap: 8px;
  padding: 7px 12px;
  font-size: .88rem;
  cursor: pointer;
  text-transform: none;
  font-weight: 400;
  letter-spacing: 0;
  color: var(--ink);
}
.ms-panel label:hover { background: var(--soft); }
.ms-panel input { margin-top: 2px; flex: 0 0 auto; }
.filters .search-field input {
  padding: 9px 10px;
  border: 1px solid var(--line);
  background: #fff;
  color: var(--ink);
  font-size: .9rem;
  min-height: 40px;
  width: 100%;
}
.filters .actions {
  display: flex; align-items: flex-end; gap: 8px;
  grid-column: 1 / -1;
}
.filters .actions button {
  padding: 9px 14px;
  border: 0;
  background: var(--blue);
  color: #fff;
  font-weight: 600;
  cursor: pointer;
}
.filters .actions button:hover { background: var(--blue-dark); }
.filters .actions button.ghost {
  background: #fff;
  border: 1px solid var(--line);
  color: var(--muted);
}

.table-wrap {
  overflow: auto;
  border: 1px solid var(--line);
  max-height: 520px;
}
table.data {
  width: 100%;
  border-collapse: collapse;
  font-size: .88rem;
}
table.data th, table.data td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--line);
  text-align: left;
  white-space: nowrap;
}
table.data th {
  position: sticky; top: 0;
  background: var(--soft);
  font-weight: 600;
  z-index: 1;
}
table.data th.sortable {
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}
table.data th.sortable:hover { color: var(--blue); }
table.data th .sort-ind {
  margin-left: 4px;
  color: var(--muted);
  font-weight: 500;
}
table.data th.sorted .sort-ind { color: var(--blue); }
table.data tr:hover td { background: #fafafa; }
table.data td.num { text-align: right; font-variant-numeric: tabular-nums; }

.table-foot {
  display: flex; flex-wrap: wrap; justify-content: space-between; gap: 12px;
  align-items: center;
  margin-top: 12px;
  color: var(--muted);
  font-size: .9rem;
}
.pager { display: flex; gap: 8px; align-items: center; }
.pager button {
  border: 1px solid var(--line);
  background: #fff;
  padding: 7px 12px;
  cursor: pointer;
  font-weight: 500;
}
.pager button:disabled { opacity: .4; cursor: not-allowed; }

.download-box {
  margin-top: 8px;
  padding: 4px 16px 16px;
  border: 1px solid var(--line);
  background: var(--soft);
}
.download-row {
  padding: 14px 0;
  border-bottom: 1px solid var(--line);
}
.download-row:last-child { border-bottom: 0; padding-bottom: 0; }
.download-row h4 {
  margin: 0 0 10px;
  font-size: .95rem;
  font-weight: 600;
}
.exports {
  display: flex; flex-wrap: wrap; gap: 8px;
  align-items: center;
}
.exports a {
  display: inline-flex; align-items: center;
  padding: 8px 12px;
  border: 1px solid var(--line);
  background: #fff;
  font-weight: 600;
  font-size: .85rem;
  letter-spacing: .02em;
  cursor: pointer;
  color: inherit;
}
.exports a:hover {
  border-color: var(--blue); color: var(--blue);
}
.exports .api-note {
  flex-basis: 100%;
  color: var(--muted);
  font-size: .78rem;
  margin-top: 4px;
  word-break: break-all;
}
.exports code { font-size: .76rem; }

.meta-fold {
  margin-top: 8px;
  border: 1px solid var(--line);
  background: #fff;
  padding: 0;
}
.meta-fold > summary {
  cursor: pointer;
  list-style: none;
  padding: 14px 16px;
  font-size: 1.05rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.meta-fold > summary::-webkit-details-marker { display: none; }
.meta-fold > summary::after {
  content: "▾";
  color: var(--muted);
  font-weight: 500;
  transition: transform .15s ease;
}
.meta-fold[open] > summary::after { transform: rotate(180deg); }
.meta-fold > summary:hover { background: var(--soft); }
.meta-fold-body {
  padding: 0 16px 16px;
  border-top: 1px solid var(--line);
}
.meta-long {
  display: grid; gap: 14px;
  padding-top: 14px;
}
.meta-group {
  border: 1px solid var(--line);
  background: #fff;
}
.meta-group-title {
  margin: 0;
  padding: 10px 12px;
  font-size: 1rem;
  font-weight: 600;
  background: var(--soft);
  border-bottom: 1px solid var(--line);
}
.meta-group .meta-vars { display: grid; gap: 0; }
.meta-var {
  border-bottom: 1px solid var(--line);
  padding: 0;
  overflow: visible;
}
.meta-var:last-child { border-bottom: 0; }
.meta-var > summary {
  cursor: pointer;
  list-style: none;
  padding: 8px 12px;
  display: grid;
  gap: 2px;
}
.meta-var > summary::-webkit-details-marker { display: none; }
.meta-var > summary:hover { background: #fafafa; }
.meta-var-name {
  font-weight: 600;
  font-size: .95rem;
}
.meta-var-desc {
  color: var(--ink);
  font-size: .95rem;
  font-weight: 400;
}
.meta-var .body {
  margin: 0;
  padding: 0 12px 10px;
  color: var(--muted);
  font-size: .88rem;
  white-space: normal;
  overflow: visible;
  max-height: none;
}
.meta-long .kv {
  display: grid;
  grid-template-columns: minmax(100px, 140px) 1fr;
  gap: 2px 12px;
  margin-bottom: 4px;
  align-items: start;
}
.meta-long .kv b {
  color: var(--muted);
  font-weight: 600;
  font-size: .78rem;
  line-height: 1.4;
  padding-top: 1px;
}
.meta-long .kv span {
  color: var(--ink);
  font-size: .95rem;
  line-height: 1.4;
  white-space: pre-wrap;
  overflow: visible;
  word-break: break-word;
}
.meta-tabs { margin-top: 2px; }
.meta-tablist {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 6px;
  border-bottom: 1px solid var(--line);
  padding-bottom: 4px;
}
.meta-tab {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  border: 1px solid transparent;
  border-radius: 4px 4px 0 0;
  background: transparent;
  cursor: pointer;
  color: var(--muted);
  font-weight: 500;
  font-size: .88rem;
}
.meta-tab img {
  width: 22px; height: 16px;
  object-fit: cover;
  border-radius: 2px;
  border: 1px solid var(--line);
}
.meta-tab:hover { color: var(--ink); background: var(--soft); }
.meta-tab.active {
  color: var(--ink);
  background: #fff;
  border-color: var(--line);
  border-bottom-color: #fff;
  box-shadow: 0 1px 0 #fff;
  font-weight: 600;
}
.meta-tabpanel { display: none; overflow: visible; }
.meta-tabpanel.active { display: block; }
.meta-footnote {
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px dashed var(--line);
  font-size: .82rem;
  line-height: 1.45;
  color: var(--muted);
  font-style: italic;
}
.archive-link {
  display: inline-block;
  margin-top: 10px;
  font-size: .9rem;
  font-weight: 600;
  color: var(--blue);
  text-decoration: underline;
  text-underline-offset: 2px;
}
.archive-link:hover { color: var(--blue-dark); }
.meta-downloads { margin-top: 16px; }

.coming {
  padding: 28px 20px;
  text-align: center;
  color: var(--muted);
  border: 1px dashed var(--line);
  background: var(--soft);
  margin-top: 8px;
}
.coming strong { color: var(--ink); display: block; margin-bottom: 6px; }

.status {
  padding: 12px 14px;
  background: #f0faf4;
  border: 1px solid #b9e0c8;
  color: #1f6b3f;
  margin-bottom: 12px;
  font-size: .9rem;
}
.status.error { background: #fff1f0; border-color: #f0c2c0; color: #8a2f2a; }
.loading {
  display: inline-flex; align-items: center; gap: 10px; color: var(--muted);
}
.spinner {
  width: 16px; height: 16px; border: 2px solid var(--line);
  border-top-color: var(--blue); border-radius: 50%;
  animation: spin .8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

@media (max-width: 720px) {
  .page-bar { flex-direction: column; align-items: stretch; }
  .lang { align-self: flex-end; margin-top: 0; }
}
</style>
</head>
<body>
<main class="wrap">
  <div class="page-bar">
    <h1 class="page-title" data-i18n="page.title"></h1>
__LANG_SWITCHER__
  </div>
  <nav class="theme-nav" aria-label="Themen">
    <a href="#bevoelkerung">
      __ICON_BEVOELKERUNG__
      <span data-i18n="theme.bevoelkerung"></span>
    </a>
    <a href="#wirtschaft">
      __ICON_WIRTSCHAFT__
      <span data-i18n="theme.wirtschaft"></span>
    </a>
    <a class="soon" href="#gesellschaft">
      __ICON_GESELLSCHAFT__
      <span data-i18n="theme.gesellschaft"></span>
    </a>
    <a class="soon" href="#umwelt">
      __ICON_UMWELT__
      <span data-i18n="theme.umwelt"></span>
    </a>
  </nav>

  <section class="theme" id="bevoelkerung" data-dataset="100550" data-topic="1_Bevölkerung" data-payload="bevoelkerung_gz">
    <h2 data-i18n="theme.bevoelkerung"></h2>
    <p class="theme-desc" data-i18n="desc.bevoelkerung"></p>

    <h3 class="block-title" data-i18n="filters.title"></h3>
    <div class="filters" data-filters></div>
    <div data-status hidden></div>

    <h3 class="block-title" data-i18n="table.title"></h3>
    <div class="table-wrap">
      <table class="data">
        <thead></thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="table-foot">
      <div data-info>–</div>
      <div class="pager">
        <button type="button" data-prev data-i18n="pager.prev"></button>
        <span data-page>1</span>
        <button type="button" data-next data-i18n="pager.next"></button>
      </div>
    </div>

    <h3 class="block-title" data-i18n="download.title"></h3>
    <div class="download-box">
      <div class="download-row">
        <h4 data-i18n="download.all"></h4>
        <div class="exports" data-exports-all></div>
      </div>
      <div class="download-row">
        <h4 data-i18n="download.filtered"></h4>
        <div class="exports" data-exports-filtered></div>
      </div>
      <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100550/view/?headless=true" target="_blank" rel="noopener"></a>
    </div>

    <details class="meta-fold">
      <summary data-i18n="meta.titleBlock"></summary>
      <div class="meta-fold-body">
        <div data-meta-long class="meta-long"></div>
      </div>
    </details>

    <div class="download-box meta-downloads">
      <div class="download-row">
        <h4 data-i18n="meta.shortTab"></h4>
        <div class="exports" data-exports-meta-short></div>
        <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100554/view/?headless=true" target="_blank" rel="noopener"></a>
      </div>
      <div class="download-row">
        <h4 data-i18n="meta.longTab"></h4>
        <div class="exports" data-exports-meta-long></div>
        <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100555/view/?headless=true" target="_blank" rel="noopener"></a>
      </div>
    </div>
  </section>

  <section class="theme" id="wirtschaft" data-dataset="100551" data-topic="2_Wirtschaft_Arbeit" data-payload="wirtschaft_gz">
    <h2 data-i18n="theme.wirtschaft"></h2>
    <p class="theme-desc" data-i18n="desc.wirtschaft"></p>

    <h3 class="block-title" data-i18n="filters.title"></h3>
    <div class="filters" data-filters></div>
    <div data-status hidden></div>

    <h3 class="block-title" data-i18n="table.title"></h3>
    <div class="table-wrap">
      <table class="data">
        <thead></thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="table-foot">
      <div data-info>–</div>
      <div class="pager">
        <button type="button" data-prev data-i18n="pager.prev"></button>
        <span data-page>1</span>
        <button type="button" data-next data-i18n="pager.next"></button>
      </div>
    </div>

    <h3 class="block-title" data-i18n="download.title"></h3>
    <div class="download-box">
      <div class="download-row">
        <h4 data-i18n="download.all"></h4>
        <div class="exports" data-exports-all></div>
      </div>
      <div class="download-row">
        <h4 data-i18n="download.filtered"></h4>
        <div class="exports" data-exports-filtered></div>
      </div>
      <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100551/view/?headless=true" target="_blank" rel="noopener"></a>
    </div>

    <details class="meta-fold">
      <summary data-i18n="meta.titleBlock"></summary>
      <div class="meta-fold-body">
        <div data-meta-long class="meta-long"></div>
      </div>
    </details>

    <div class="download-box meta-downloads">
      <div class="download-row">
        <h4 data-i18n="meta.shortTab"></h4>
        <div class="exports" data-exports-meta-short></div>
        <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100554/view/?headless=true" target="_blank" rel="noopener"></a>
      </div>
      <div class="download-row">
        <h4 data-i18n="meta.longTab"></h4>
        <div class="exports" data-exports-meta-long></div>
        <a class="archive-link" data-i18n="download.archive" href="https://data.bs.ch/explore/assets/100555/view/?headless=true" target="_blank" rel="noopener"></a>
      </div>
    </div>
  </section>

  <section class="theme" id="gesellschaft" data-soon="1">
    <h2 data-i18n="theme.gesellschaft"></h2>
    <p class="theme-desc" data-i18n="desc.gesellschaft"></p>
    <div class="coming"><strong data-i18n="soon.title"></strong><span data-i18n="soon.body"></span></div>
  </section>

  <section class="theme" id="umwelt" data-soon="1">
    <h2 data-i18n="theme.umwelt"></h2>
    <p class="theme-desc" data-i18n="desc.umwelt"></p>
    <div class="coming"><strong data-i18n="soon.title"></strong><span data-i18n="soon.body"></span></div>
  </section>
</main>

<script>
const PAYLOAD = __PAYLOAD__;
const FIXED_LANG = __FIXED_LANG__;
const SHOW_LANG = __SHOW_LANG__;
const PAGE_SIZE = 20;
const META_COUNTRIES = ["de", "fr", "ch"];
const LANGS = [
  { id: "de", label: "Deutsch" },
  { id: "en", label: "English" },
  { id: "fr", label: "Français" },
];

const FILTER_LABELS = {
  geo: { de: "Geo-Ebene", en: "Geo level", fr: "Niveau géo" },
  land: { de: "Land", en: "Country", fr: "Pays" },
  region: { de: "Region", en: "Region", fr: "Région" },
  kanton_kreis_departement: { de: "Kanton / Kreis / Département", en: "Canton / District / Department", fr: "Canton / Kreis / Département" },
  jahr_annee: { de: "Jahr", en: "Year", fr: "Année" },
  variable: { de: "Variable", en: "Variable", fr: "Variable" },
};

const I18N = {
  de: {
    "page.title": "StatRhena: Daten & Metadaten",
    "theme.bevoelkerung": "Bevölkerung",
    "theme.wirtschaft": "Wirtschaft & Arbeit",
    "theme.gesellschaft": "Gesellschaft",
    "theme.umwelt": "Umwelt & Mobilität",
    "desc.bevoelkerung": "Harmonisierte Bevölkerungszahlen für die Oberrheinregion: Bestand, Altersstruktur, Staatsangehörigkeit, natürliche und räumliche Bevölkerungsbewegung sowie Szenarien bis 2050. Die Daten stammen aus den amtlichen Statistiken der Schweiz, Deutschlands und Frankreichs und wurden für den Vergleich über die Grenzen hinweg aufbereitet.",
    "desc.wirtschaft": "Kennzahlen zu Arbeitsmarkt, Grenzgängerinnen und Grenzgängern, Wirtschaftsleistung und Unternehmen in der Oberrheinregion. Die Daten stammen aus den amtlichen Statistiken der Schweiz, Deutschlands und Frankreichs und wurden für den Vergleich über die Grenzen hinweg aufbereitet.",
    "desc.gesellschaft": "Gesellschaftliche Indikatoren für die Oberrheinregion – demnächst verfügbar.",
    "desc.umwelt": "Indikatoren zu Umwelt und Mobilität in der Oberrheinregion – demnächst verfügbar.",
    "filters.title": "Filter",
    "table.title": "Tabelle",
    "download.title": "Download",
    "download.all": "Alle Daten",
    "download.filtered": "Gefilterte Daten",
    "download.hintAll": "API-Export ohne Filter.",
    "download.hintFiltered": "API-Export mit aktiven Filtern (refine).",
    "download.archive": "Datenablage",
    "meta.titleBlock": "Metadaten",
    "meta.shortTab": "Metadaten kurz",
    "meta.longTab": "Metadaten lang",
    "meta.variable": "Variable",
    "meta.description": "Beschreibung",
    "meta.unit": "Einheit",
    "meta.period": "Zeitraum",
    "meta.source": "Quelle",
    "meta.footnote": "Fussnote",
    "meta.statistic": "Statistik",
    "meta.sourceTitle": "Originaltitel",
    "meta.info": "Methodik / Hinweise",
    "meta.empty": "Für diese Sprache liegen keine längeren Hinweise vor.",
    "meta.country.ch": "Schweiz",
    "meta.country.de": "Deutschland",
    "meta.country.fr": "Frankreich",
    "filter.all": "Alle",
    "filter.selected": "ausgewählt",
    "filter.search": "Suche",
    "filter.placeholder": "Textfilter…",
    "filter.apply": "Filtern",
    "filter.reset": "Zurücksetzen",
    "pager.prev": "Zurück",
    "pager.next": "Weiter",
    "pager.page": "Seite",
    "table.rows": "Zeilen",
    "table.none": "Keine Treffer für die aktuellen Filter.",
    "soon.title": "Demnächst",
    "soon.body": "Die Daten werden freigeschaltet, sobald sie vorliegen.",
    "partners.kofi": "Kofinanzierende Partner",
    "partners.asso": "Assoziierte Partner",
    "footer.copy": "© 2026 StatRhena. All rights reserved. Made with Love by",
    "footer.top": "Seitenanfang ↑",
    "legal.impressum": "Impressum",
    "legal.datenschutz": "Datenschutz",
    "legal.agb": "AGB",
    "status.loading": "Lade Daten…",
    "status.loaded": "Zeilen geladen.",
  },
  en: {
    "page.title": "StatRhena: Data & Metadata",
    "theme.bevoelkerung": "Population",
    "theme.wirtschaft": "Economy & Labour",
    "theme.gesellschaft": "Society",
    "theme.umwelt": "Environment & Mobility",
    "desc.bevoelkerung": "Harmonised population figures for the Upper Rhine region: stock, age structure, nationality, natural and migration change, and scenarios through 2050. The data come from official statistics in Switzerland, Germany and France and have been prepared for cross-border comparison.",
    "desc.wirtschaft": "Indicators on the labour market, cross-border workers, economic output and businesses in the Upper Rhine region. The data come from official statistics in Switzerland, Germany and France and have been prepared for cross-border comparison.",
    "desc.gesellschaft": "Social indicators for the Upper Rhine region – coming soon.",
    "desc.umwelt": "Environment and mobility indicators for the Upper Rhine region – coming soon.",
    "filters.title": "Filters",
    "table.title": "Table",
    "download.title": "Download",
    "download.all": "All data",
    "download.filtered": "Filtered data",
    "download.hintAll": "API export without filters.",
    "download.hintFiltered": "API export with active filters (refine).",
    "download.archive": "Data repository",
    "meta.titleBlock": "Metadata",
    "meta.shortTab": "Metadata (short)",
    "meta.longTab": "Metadata (long)",
    "meta.variable": "Variable",
    "meta.description": "Description",
    "meta.unit": "Unit",
    "meta.period": "Period",
    "meta.source": "Source",
    "meta.footnote": "Footnote",
    "meta.statistic": "Statistic",
    "meta.sourceTitle": "Original title",
    "meta.info": "Methodology / notes",
    "meta.empty": "No longer notes are available for this language.",
    "meta.country.ch": "Switzerland",
    "meta.country.de": "Germany",
    "meta.country.fr": "France",
    "filter.all": "All",
    "filter.selected": "selected",
    "filter.search": "Search",
    "filter.placeholder": "Text filter…",
    "filter.apply": "Filter",
    "filter.reset": "Reset",
    "pager.prev": "Previous",
    "pager.next": "Next",
    "pager.page": "Page",
    "table.rows": "rows",
    "table.none": "No results for the current filters.",
    "soon.title": "Coming soon",
    "soon.body": "Data will be enabled once available.",
    "partners.kofi": "Co-financing partners",
    "partners.asso": "Associated partners",
    "footer.copy": "© 2026 StatRhena. All rights reserved. Made with Love by",
    "footer.top": "Back to top ↑",
    "legal.impressum": "Imprint",
    "legal.datenschutz": "Privacy policy",
    "legal.agb": "Terms and Conditions",
    "status.loading": "Loading data…",
    "status.loaded": "rows loaded.",
  },
  fr: {
    "page.title": "StatRhena: Données & Métadonnées",
    "theme.bevoelkerung": "Population",
    "theme.wirtschaft": "Économie & Emploi",
    "theme.gesellschaft": "Société",
    "theme.umwelt": "Environnement & Mobilité",
    "desc.bevoelkerung": "Chiffres de population harmonisés pour la région du Rhin supérieur : effectifs, structure par âge, nationalité, mouvement naturel et migratoire, ainsi que scénarios jusqu’en 2050. Les données proviennent des statistiques officielles de la Suisse, de l’Allemagne et de la France et ont été préparées pour la comparaison transfrontalière.",
    "desc.wirtschaft": "Indicateurs sur le marché du travail, les travailleurs frontaliers, la performance économique et les entreprises dans la région du Rhin supérieur. Les données proviennent des statistiques officielles de la Suisse, de l’Allemagne et de la France et ont été préparées pour la comparaison transfrontalière.",
    "desc.gesellschaft": "Indicateurs sociétaux pour la région du Rhin supérieur – bientôt disponibles.",
    "desc.umwelt": "Indicateurs d’environnement et de mobilité pour la région du Rhin supérieur – bientôt disponibles.",
    "filters.title": "Filtres",
    "table.title": "Tableau",
    "download.title": "Téléchargement",
    "download.all": "Toutes les données",
    "download.filtered": "Données filtrées",
    "download.hintAll": "Export API sans filtre.",
    "download.hintFiltered": "Export API avec filtres actifs (refine).",
    "download.archive": "Dépôt de données",
    "meta.titleBlock": "Métadonnées",
    "meta.shortTab": "Métadonnées (courtes)",
    "meta.longTab": "Métadonnées (longues)",
    "meta.variable": "Variable",
    "meta.description": "Description",
    "meta.unit": "Unité",
    "meta.period": "Période",
    "meta.source": "Source",
    "meta.footnote": "Note",
    "meta.statistic": "Statistique",
    "meta.sourceTitle": "Titre original",
    "meta.info": "Méthodologie / notes",
    "meta.empty": "Aucune note longue n’est disponible pour cette langue.",
    "meta.country.ch": "Suisse",
    "meta.country.de": "Allemagne",
    "meta.country.fr": "France",
    "filter.all": "Tous",
    "filter.selected": "sélectionné(s)",
    "filter.search": "Recherche",
    "filter.placeholder": "Filtre texte…",
    "filter.apply": "Filtrer",
    "filter.reset": "Réinitialiser",
    "pager.prev": "Précédent",
    "pager.next": "Suivant",
    "pager.page": "Page",
    "table.rows": "lignes",
    "table.none": "Aucun résultat pour les filtres actuels.",
    "soon.title": "Bientôt",
    "soon.body": "Les données seront activées dès qu’elles seront disponibles.",
    "partners.kofi": "Partenaires cofinanceurs",
    "partners.asso": "Partenaires associés",
    "footer.copy": "© 2026 StatRhena. All rights reserved. Made with Love by",
    "footer.top": "Haut de page ↑",
    "legal.impressum": "Mentions légales",
    "legal.datenschutz": "Politique de confidentialité",
    "legal.agb": "CGV",
    "status.loading": "Chargement des données…",
    "status.loaded": "lignes chargées.",
  },
};

const LEGAL = {
  de: {
    impressum: "https://stala.wedeon.info/impressum/",
    datenschutz: "https://stala.wedeon.info/datenschutz/",
    agb: "https://stala.wedeon.info/allgemeine-geschaeftsbedingungen/",
  },
  en: {
    impressum: "https://stala.wedeon.info/en/impressum/",
    datenschutz: "https://stala.wedeon.info/en/datenschutz/",
    agb: "https://stala.wedeon.info/en/allgemeine-geschaeftsbedingungen/",
  },
  fr: {
    impressum: "https://stala.wedeon.info/fr/impressum/",
    datenschutz: "https://stala.wedeon.info/fr/datenschutz/",
    agb: "https://stala.wedeon.info/fr/allgemeine-geschaeftsbedingungen/",
  },
};

const FILTER_COLS = ["geo", "land", "region", "kanton_kreis_departement", "jahr_annee", "variable"];
const SEARCH_COLS = ["geo", "land", "region", "kanton_kreis_departement", "variable", "variable_desription"];
const META_SHORT_ID = "100554";
const META_LONG_ID = "100555";

const state = {
  lang: FIXED_LANG || localStorage.getItem("statrhena_lang") || "de",
  metadata: null,
  controllers: [],
};

function t(key) {
  return (I18N[state.lang] && I18N[state.lang][key]) || I18N.de[key] || key;
}
function filterLabel(col) {
  return (FILTER_LABELS[col] && FILTER_LABELS[col][state.lang]) || col;
}
function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
function escapeAttr(s) { return escapeHtml(s).replaceAll("'", "&#39;"); }
function fmtNum(n) {
  return new Intl.NumberFormat(state.lang === "fr" ? "fr-FR" : state.lang === "en" ? "en-GB" : "de-CH").format(n);
}

function b64ToBytes(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}
async function gunzipB64(b64) {
  const bytes = b64ToBytes(b64);
  if (typeof DecompressionStream === "undefined") throw new Error("DecompressionStream unsupported");
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return await new Response(stream).text();
}

function parseCSV(text) {
  const rows = [];
  let i = 0, field = "", row = [], inQuotes = false;
  const pushField = () => { row.push(field); field = ""; };
  const pushRow = () => {
    if (row.length === 1 && row[0] === "" && rows.length) { row = []; return; }
    rows.push(row); row = [];
  };
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ",") { pushField(); i++; continue; }
    if (c === "\n") { pushField(); pushRow(); i++; continue; }
    if (c === "\r") { i++; continue; }
    field += c; i++;
  }
  if (field.length || row.length) { pushField(); pushRow(); }
  if (!rows.length) return { headers: [], records: [] };
  const headers = rows[0].map(h => h.trim());
  const records = [];
  for (let r = 1; r < rows.length; r++) {
    const obj = {};
    const vals = rows[r];
    let empty = true;
    for (let c = 0; c < headers.length; c++) {
      const v = (vals[c] ?? "").trim();
      obj[headers[c]] = v === "NA" ? "" : v;
      if (v) empty = false;
    }
    if (!empty) records.push(obj);
  }
  return { headers, records };
}

/** filters: { field: string[] } */
function buildExportUrl(id, format, filters = {}, search = "") {
  const params = new URLSearchParams();
  params.set("timezone", "Europe/Zurich");
  Object.entries(filters).forEach(([field, values]) => {
    (values || []).forEach(value => {
      if (value) params.append("refine", `${field}:${value}`);
    });
  });
  if (search && search.trim()) {
    const q = search.trim().replaceAll('"', '\\"');
    params.set("where", `search("${q}")`);
  }
  return `https://data.bs.ch/api/explore/v2.1/catalog/datasets/${id}/exports/${format}/?${params.toString()}`;
}

function formatButtons(id, filters, search) {
  const formats = [["csv","CSV"],["json","JSON"],["xlsx","XLSX"],["parquet","Parquet"]];
  return formats.map(([fmt, label]) =>
    `<a href="${buildExportUrl(id, fmt, filters, search)}" target="_blank" rel="noopener">${label}</a>`
  ).join("");
}

function applyI18n() {
  document.documentElement.lang = state.lang;
  document.title = t("page.title");
  document.querySelectorAll("[data-i18n]").forEach(el => {
    el.textContent = t(el.getAttribute("data-i18n"));
  });
  if (SHOW_LANG) {
    const cur = LANGS.find(l => l.id === state.lang);
    document.getElementById("langFlag").src = PAYLOAD.flags[state.lang];
    document.getElementById("langLabel").textContent = cur.label;
  }
  renderMetadata();
  renderMetaDownloads();
  state.controllers.forEach(ctrl => ctrl.refreshI18n());
}

function renderLangMenu() {
  if (!SHOW_LANG) return;
  const menu = document.getElementById("langMenu");
  menu.innerHTML = LANGS.map(l =>
    `<button type="button" role="option" data-lang="${l.id}" class="${l.id === state.lang ? "active" : ""}">
      <img src="${PAYLOAD.flags[l.id]}" alt="" /> ${escapeHtml(l.label)}
    </button>`
  ).join("");
  menu.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      state.lang = btn.dataset.lang;
      localStorage.setItem("statrhena_lang", state.lang);
      document.getElementById("langSwitch").classList.remove("open");
      document.getElementById("langBtn").setAttribute("aria-expanded", "false");
      renderLangMenu();
      applyI18n();
    });
  });
}

function initLangSwitcher() {
  if (!SHOW_LANG) return;
  renderLangMenu();
  const langBtn = document.getElementById("langBtn");
  const langSwitch = document.getElementById("langSwitch");
  langBtn.addEventListener("click", () => {
    const open = langSwitch.classList.toggle("open");
    langBtn.setAttribute("aria-expanded", open ? "true" : "false");
  });
  document.addEventListener("click", (e) => {
    if (!langSwitch.contains(e.target)) {
      langSwitch.classList.remove("open");
      langBtn.setAttribute("aria-expanded", "false");
    }
  });
}

function countryPanel(r, c) {
  const srcTitle = r.titles[c];
  const info = r.info[c];
  const stat = r.stats[c];
  const src = r.sources[c];
  const ts = (r.timespan[c] || []).filter(Boolean).join("–");
  if (!srcTitle && !info && !stat && !src && !ts) {
    return `<div class="kv"><b></b><span>${escapeHtml(t("meta.empty"))}</span></div>`;
  }
  return `${srcTitle ? `<div class="kv"><b>${escapeHtml(t("meta.sourceTitle"))}</b><span>${escapeHtml(srcTitle)}</span></div>` : ""}
    ${stat ? `<div class="kv"><b>${escapeHtml(t("meta.statistic"))}</b><span>${escapeHtml(stat)}</span></div>` : ""}
    ${src ? `<div class="kv"><b>${escapeHtml(t("meta.source"))}</b><span>${escapeHtml(src)}</span></div>` : ""}
    ${ts ? `<div class="kv"><b>${escapeHtml(t("meta.period"))}</b><span>${escapeHtml(ts)}</span></div>` : ""}
    ${info ? `<div class="kv"><b>${escapeHtml(t("meta.info"))}</b><span>${escapeHtml(info)}</span></div>` : ""}`;
}

function renderMetadata() {
  if (!state.metadata) return;
  document.querySelectorAll("[data-meta-long]").forEach(longEl => {
    const topic = longEl.closest("section").dataset.topic;
    const bundle = state.metadata[topic];
    if (!bundle) return;
    const byTitle = new Map();
    for (const r of bundle.long) {
      const key = r.title || r.variable;
      if (!byTitle.has(key)) byTitle.set(key, []);
      byTitle.get(key).push(r);
    }
    longEl.innerHTML = [...byTitle.entries()].map(([title, rows]) => {
      const vars = rows.map(r => {
        const tabs = META_COUNTRIES.map((c, i) =>
          `<button type="button" class="meta-tab ${i === 0 ? "active" : ""}" role="tab" data-country="${c}" aria-selected="${i === 0 ? "true" : "false"}">
            <img src="${PAYLOAD.flags[c]}" alt="" />
            <span>${escapeHtml(t("meta.country." + c))}</span>
          </button>`
        ).join("");
        const panels = META_COUNTRIES.map((c, i) =>
          `<div class="meta-tabpanel ${i === 0 ? "active" : ""}" role="tabpanel" data-country="${c}">${countryPanel(r, c)}</div>`
        ).join("");
        const footnote = r.footnote
          ? `<div class="meta-footnote">${escapeHtml(r.footnote)}</div>`
          : "";
        return `<details class="meta-var">
          <summary>
            <span class="meta-var-name">${escapeHtml(r.variable)}</span>
            <span class="meta-var-desc">${escapeHtml(r.description || "")}</span>
          </summary>
          <div class="body">
            <div class="meta-tabs">
              <div class="meta-tablist" role="tablist">${tabs}</div>
              ${panels}
            </div>
            ${footnote}
          </div>
        </details>`;
      }).join("");
      return `<div class="meta-group">
        <h4 class="meta-group-title">${escapeHtml(title)}</h4>
        <div class="meta-vars">${vars}</div>
      </div>`;
    }).join("");

    longEl.querySelectorAll(".meta-tabs").forEach(tabs => {
      const tabBtns = tabs.querySelectorAll(".meta-tab");
      const panels = tabs.querySelectorAll(".meta-tabpanel");
      tabBtns.forEach(btn => {
        btn.addEventListener("click", () => {
          const c = btn.dataset.country;
          tabBtns.forEach(b => {
            const on = b.dataset.country === c;
            b.classList.toggle("active", on);
            b.setAttribute("aria-selected", on ? "true" : "false");
          });
          panels.forEach(p => p.classList.toggle("active", p.dataset.country === c));
        });
      });
    });
  });
}

function renderMetaDownloads() {
  document.querySelectorAll("section[data-topic]").forEach(section => {
    const filters = { topic: [section.dataset.topic] };
    const shortEl = section.querySelector("[data-exports-meta-short]");
    const longEl = section.querySelector("[data-exports-meta-long]");
    if (!shortEl || !longEl) return;
    const shortUrl = buildExportUrl(META_SHORT_ID, "csv", filters, "");
    const longUrl = buildExportUrl(META_LONG_ID, "csv", filters, "");
    shortEl.innerHTML = formatButtons(META_SHORT_ID, filters, "") +
      `<div class="api-note"><code>${escapeHtml(shortUrl)}</code></div>`;
    longEl.innerHTML = formatButtons(META_LONG_ID, filters, "") +
      `<div class="api-note"><code>${escapeHtml(longUrl)}</code></div>`;
  });
}

function uniqueValues(records, col) {
  const set = new Set();
  for (const r of records) if (r[col]) set.add(r[col]);
  return [...set].sort((a, b) => a.localeCompare(b, "de", { numeric: true }));
}

function createTableController(section, data) {
  const datasetId = section.dataset.dataset;
  const ctrl = {
    filters: Object.fromEntries(FILTER_COLS.map(c => [c, []])),
    search: "",
    page: 1,
    sortCol: null,
    sortDir: "asc",
    filtered: data.records.slice(),
    options: Object.fromEntries(FILTER_COLS.map(c => [c, uniqueValues(data.records, c)])),

    refreshI18n() {
      this.renderFilters();
      this.renderExports();
      this.render();
    },

    summaryText(col) {
      const vals = this.filters[col] || [];
      if (!vals.length) return t("filter.all");
      if (vals.length === 1) return vals[0];
      return `${vals.length} ${t("filter.selected")}`;
    },

    renderExports() {
      const allEl = section.querySelector("[data-exports-all]");
      const filEl = section.querySelector("[data-exports-filtered]");
      allEl.innerHTML = formatButtons(datasetId, {}, "") +
        `<div class="api-note">${escapeHtml(t("download.hintAll"))}<br><code>${escapeHtml(buildExportUrl(datasetId, "csv"))}</code></div>`;
      filEl.innerHTML = formatButtons(datasetId, this.filters, this.search) +
        `<div class="api-note">${escapeHtml(t("download.hintFiltered"))}<br><code>${escapeHtml(buildExportUrl(datasetId, "csv", this.filters, this.search))}</code></div>`;
    },

    sortRows() {
      if (!this.sortCol) return;
      const col = this.sortCol;
      const dir = this.sortDir === "asc" ? 1 : -1;
      this.filtered.sort((a, b) => {
        const av = a[col] ?? "";
        const bv = b[col] ?? "";
        const an = parseFloat(String(av).replace(",", "."));
        const bn = parseFloat(String(bv).replace(",", "."));
        if (!Number.isNaN(an) && !Number.isNaN(bn) && String(av).trim() !== "" && String(bv).trim() !== "") {
          return (an - bn) * dir;
        }
        return String(av).localeCompare(String(bv), "de", { numeric: true, sensitivity: "base" }) * dir;
      });
    },

    toggleSort(col) {
      if (this.sortCol === col) {
        this.sortDir = this.sortDir === "asc" ? "desc" : "asc";
      } else {
        this.sortCol = col;
        this.sortDir = "asc";
      }
      this.sortRows();
      this.page = 1;
      this.render();
    },

    renderFilters() {
      const el = section.querySelector("[data-filters]");
      const parts = FILTER_COLS.map(col => {
        const selected = new Set(this.filters[col] || []);
        const opts = this.options[col].map(v =>
          `<label><input type="checkbox" value="${escapeAttr(v)}" ${selected.has(v) ? "checked" : ""} /><span>${escapeHtml(v)}</span></label>`
        ).join("");
        const has = selected.size > 0;
        return `<div class="filter-field">
          <div class="flabel" title="${escapeAttr(col)}">${escapeHtml(filterLabel(col))}</div>
          <div class="ms" data-col="${escapeAttr(col)}">
            <button type="button" class="ms-btn" aria-haspopup="listbox">
              <span class="ms-summary ${has ? "has" : ""}">${escapeHtml(this.summaryText(col))}</span>
              <span class="ms-caret">▾</span>
            </button>
            <div class="ms-panel" role="listbox">${opts}</div>
          </div>
        </div>`;
      });
      parts.push(`<div class="filter-field search-field">
        <div class="flabel">${escapeHtml(t("filter.search"))}</div>
        <input type="search" data-search placeholder="${escapeAttr(t("filter.placeholder"))}" value="${escapeAttr(this.search)}" />
      </div>`);
      parts.push(`<div class="actions">
        <button type="button" data-apply>${escapeHtml(t("filter.apply"))}</button>
        <button type="button" class="ghost" data-reset>${escapeHtml(t("filter.reset"))}</button>
      </div>`);
      el.innerHTML = parts.join("");

      el.querySelectorAll(".ms").forEach(ms => {
        const col = ms.dataset.col;
        const btn = ms.querySelector(".ms-btn");
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const wasOpen = ms.classList.contains("open");
          el.querySelectorAll(".ms").forEach(m => m.classList.remove("open"));
          if (!wasOpen) ms.classList.add("open");
        });
        ms.querySelectorAll('input[type="checkbox"]').forEach(cb => {
          cb.addEventListener("change", () => {
            this.filters[col] = [...ms.querySelectorAll('input[type="checkbox"]:checked')].map(x => x.value);
            const sum = ms.querySelector(".ms-summary");
            sum.textContent = this.summaryText(col);
            sum.classList.toggle("has", this.filters[col].length > 0);
            this.apply();
          });
        });
      });
      el.querySelector("[data-search]").addEventListener("input", e => { this.search = e.target.value; });
      el.querySelector("[data-apply]").addEventListener("click", () => this.apply());
      el.querySelector("[data-reset]").addEventListener("click", () => {
        this.filters = Object.fromEntries(FILTER_COLS.map(c => [c, []]));
        this.search = "";
        this.page = 1;
        this.renderFilters();
        this.apply();
      });
    },

    apply() {
      const q = this.search.trim().toLowerCase();
      this.filtered = data.records.filter(row => {
        for (const col of FILTER_COLS) {
          const vals = this.filters[col] || [];
          if (vals.length && !vals.includes(row[col])) return false;
        }
        if (!q) return true;
        return SEARCH_COLS.some(col => String(row[col] || "").toLowerCase().includes(q));
      });
      this.sortRows();
      this.page = 1;
      this.renderExports();
      this.render();
    },

    render() {
      const thead = section.querySelector("thead");
      const tbody = section.querySelector("tbody");
      const info = section.querySelector("[data-info]");
      const pageLabel = section.querySelector("[data-page]");
      const prev = section.querySelector("[data-prev]");
      const next = section.querySelector("[data-next]");
      thead.innerHTML = `<tr>${data.headers.map(h => {
        const sorted = this.sortCol === h;
        const arrow = !sorted ? "↕" : (this.sortDir === "asc" ? "↑" : "↓");
        return `<th class="sortable ${sorted ? "sorted" : ""}" data-sort="${escapeAttr(h)}">${escapeHtml(h)}<span class="sort-ind">${arrow}</span></th>`;
      }).join("")}</tr>`;
      thead.querySelectorAll("th[data-sort]").forEach(th => {
        th.addEventListener("click", () => this.toggleSort(th.dataset.sort));
      });
      const total = this.filtered.length;
      const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
      if (this.page > pages) this.page = pages;
      const start = (this.page - 1) * PAGE_SIZE;
      const slice = this.filtered.slice(start, start + PAGE_SIZE);
      const numericHints = new Set(["values_integer", "values_decimal", "jahr_annee"]);
      tbody.innerHTML = slice.map(row =>
        `<tr>${data.headers.map(h => {
          const v = row[h] ?? "";
          const cls = numericHints.has(h) || /^-?\d+(\.\d+)?$/.test(v) ? "num" : "";
          return `<td class="${cls}">${escapeHtml(v)}</td>`;
        }).join("")}</tr>`
      ).join("") || `<tr><td colspan="${data.headers.length}">${escapeHtml(t("table.none"))}</td></tr>`;
      info.textContent = `${fmtNum(total)} ${t("table.rows")} · ${t("pager.page")} ${this.page} / ${pages}`;
      pageLabel.textContent = `${t("pager.page")} ${this.page}`;
      prev.disabled = this.page <= 1;
      next.disabled = this.page >= pages;
      prev.onclick = () => { if (this.page > 1) { this.page--; this.render(); } };
      next.onclick = () => { this.page++; this.render(); };
    },
  };

  document.addEventListener("click", () => {
    section.querySelectorAll(".ms").forEach(m => m.classList.remove("open"));
  });

  ctrl.renderFilters();
  ctrl.renderExports();
  ctrl.render();
  state.controllers.push(ctrl);
  return ctrl;
}

async function loadSection(section) {
  const status = section.querySelector("[data-status]");
  status.hidden = false;
  status.className = "status";
  status.innerHTML = `<span class="loading"><span class="spinner"></span> ${escapeHtml(t("status.loading"))}</span>`;
  try {
    const text = await gunzipB64(PAYLOAD[section.dataset.payload]);
    const parsed = parseCSV(text);
    status.innerHTML = `${fmtNum(parsed.records.length)} ${escapeHtml(t("status.loaded"))}`;
    setTimeout(() => { status.hidden = true; }, 2500);
    createTableController(section, parsed);
  } catch (err) {
    status.className = "status error";
    status.textContent = String(err.message || err);
  }
}

async function boot() {
  initLangSwitcher();

  try {
    state.metadata = JSON.parse(await gunzipB64(PAYLOAD.metadata_gz));
  } catch (err) {
    console.error(err);
  }

  applyI18n();

  for (const section of document.querySelectorAll("section[data-payload]")) {
    await loadSection(section);
  }
}

boot().catch(console.error);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
