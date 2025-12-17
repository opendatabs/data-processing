import logging
import os
import shutil
from pathlib import Path

import common
import pandas as pd

import fitz

DATA_ORIG_PATH = "data_orig"


def sanitize_filename(name: str) -> str:
    transl_table = str.maketrans({"ä": "ae", "Ä": "Ae", "ö": "oe", "Ö": "Oe", "ü": "ue", "Ü": "Ue", "ß": "ss"})
    name = name.translate(transl_table).replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return "".join(c for c in name if c in allowed)


def flatten_pdf_to_image_pdf(src_pdf: str, dst_pdf: str, dpi: int = 200) -> None:
    """
    Creates a new PDF where each page is a rasterized image of the original page.
    Result: text is no longer selectable/copyable (but OCR is still possible).
    """
    src = fitz.open(src_pdf)
    out = fitz.open()

    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)

    for i, page in enumerate(src, start=1):
        pix = page.get_pixmap(matrix=mat, alpha=False)
        rect = page.rect

        out_page = out.new_page(width=rect.width, height=rect.height)

        img_rect = fitz.Rect(0, 0, rect.width, rect.height)
        out_page.insert_image(img_rect, pixmap=pix)

        if i % 10 == 0:
            logging.info(f"Flattening {os.path.basename(src_pdf)}: page {i}/{src.page_count}")

    Path(dst_pdf).parent.mkdir(parents=True, exist_ok=True)
    out.save(dst_pdf, deflate=True)
    out.close()
    src.close()


def process_excel_file():
    excel_filename = "Liste_Gutachten.xlsx"
    excel_file_path = os.path.join(DATA_ORIG_PATH, excel_filename)
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"The file '{excel_filename}' does not exist in the directory '{DATA_ORIG_PATH}'.")

    df = pd.read_excel(excel_file_path)
    df["Dateiname"] = df["Dateiname"].astype(str)
    # Neue Spalte: Dateiname wie er auf dem FTP erscheinen soll
    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)
    base_url = "https://data-bs.ch/stata/staka/gutachten/"
    df["URL_Datei"] = base_url + df["Dateiname_ftp"]

    # Check: existieren alle lokalen Dateien mit Originalnamen?
    files_in_data_orig = set(os.listdir(DATA_ORIG_PATH))
    listed_files = set(df["Dateiname"])
    unlisted_files = files_in_data_orig - listed_files - {".gitkeep", "Liste_Gutachten.xlsx"}
    if unlisted_files:
        raise ValueError(f"The following files are in 'data_orig' but not in 'Liste_Gutachten': {unlisted_files}")
    missing_files = listed_files - files_in_data_orig
    if missing_files:
        raise ValueError(
            f"The following files are listed in 'Liste_Gutachten' but do not exist in 'data_orig': {missing_files}"
        )

    logging.info("All files in 'data_orig' are listed in 'Liste_Gutachten' and vice versa.")
    return df


def upload_files_to_ftp(df: pd.DataFrame):
    remote_dir = "staka/gutachten/"
    os.makedirs("data", exist_ok=True)

    for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"]):
        src_path = os.path.join(DATA_ORIG_PATH, orig_name)
        dst_path = os.path.join("data", ftp_name)

        ext = Path(orig_name).suffix.lower()

        if ext == ".pdf":
            # Ensure the output name ends with .pdf
            if Path(dst_path).suffix.lower() != ".pdf":
                dst_path = str(Path(dst_path).with_suffix(".pdf"))
                ftp_name = str(Path(ftp_name).with_suffix(".pdf"))

            flatten_pdf_to_image_pdf(src_path, dst_path, dpi=200)
            logging.info(f"Flattened PDF {orig_name} -> {ftp_name}")
        else:
            shutil.copy2(src_path, dst_path)

        common.upload_ftp(dst_path, remote_path=remote_dir)
        logging.info(f"Uploaded {orig_name} as {ftp_name} to FTP at {remote_dir}")

    csv_filename = "100489_gutachten.csv"
    csv_file_path = os.path.join("data", csv_filename)
    df_out = df.drop(columns=["Dateiname_ftp"])
    df_out.to_csv(csv_file_path, index=False)
    common.update_ftp_and_odsp(csv_file_path, remote_dir, dataset_id="100489")


def main():
    df = process_excel_file()
    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful.")
