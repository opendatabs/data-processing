import logging
import os
import re

import common
import markdown
import pandas as pd
from markdown_newtab import NewTabExtension


def split_markdown_image(md):
    """Extract alt text and image source from markdown image syntax."""
    match = re.match(r"!\[(.*?)\]\((.*?)\)", str(md).strip())
    if match:
        return match.group(1), match.group(2)
    return None, None


def split_markdown_links(md):
    """Extract display text and URLs from markdown link syntax, handling multiple links."""
    parts = [p.strip() for p in str(md).split(";") if p.strip()]
    anzeigetexte, links = [], []
    for p in parts:
        match = re.match(r"\[(.*?)\]\((.*?)\)", p)
        if match:
            anzeigetexte.append(match.group(1))
            links.append(match.group(2))
        else:
            anzeigetexte.append("")
            links.append("")
    return ";".join(anzeigetexte), ";".join(links)


def main():
    file_path = os.path.join("data_orig", "Events St. Jakob.xlsx")
    # Read the Sheet with the event data
    df_eventliste = pd.read_excel(file_path, sheet_name="Eventliste")
    df_eventliste["Info_Text_HTML"] = df_eventliste["Info_Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    # Read the Sheet with the "Anreiseempfehlung" data
    df_anreiseempf = pd.read_excel(file_path, sheet_name="Anreiseempfehlung")
    df_anreiseempf["Text_HTML"] = df_anreiseempf["Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    df_anreiseempf[["Alt-Texte", "Bildquellen"]] = df_anreiseempf["Bilder"].apply(
        lambda x: pd.Series(split_markdown_image(x))
    )

    df_anreiseempf[["Link_Anzeigetexte", "Links"]] = df_anreiseempf["Weiterfuehrende Links"].apply(
        lambda x: pd.Series(split_markdown_links(x))
    )
    df_zeitraum_info = pd.read_excel(file_path, sheet_name="Zeitraum_Info")
    df_zeitraum_info["Text_HTML"] = df_zeitraum_info["Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    # Remove the original columns that are no longer needed
    df_anreiseempf.drop(columns=["Bilder", "Weiterfuehrende Links", "Text"], inplace=True)
    df_eventliste.drop(columns=["Info_Text"], inplace=True)
    df_zeitraum_info.drop(columns=["Text"], inplace=True)

    path_eventliste = os.path.join("data", "eventliste_stjakob.csv")
    path_anreiseempf = os.path.join("data", "anreiseempfehlung_stjakob.csv")
    path_zeitraum_info = os.path.join("data", "zeitraum_info_stjakob.csv")
    df_eventliste.to_csv(path_eventliste, index=False)
    df_anreiseempf.to_csv(path_anreiseempf, index=False)
    df_zeitraum_info.to_csv(path_zeitraum_info, index=False)
    common.update_ftp_and_odsp(path_eventliste, "kapo/eventverkehr_st.jakob", "100419")
    common.update_ftp_and_odsp(path_anreiseempf, "kapo/eventverkehr_st.jakob", "100429")
    common.update_ftp_and_odsp(path_zeitraum_info, "kapo/eventverkehr_st.jakob", "100464")

    # Upload PNG images to FTP
    png_dir = os.path.join("data_orig", "PNG_Anfahrtsempfehlungen")
    remote_png_dir = "kapo/eventverkehr_st.jakob/png_anfahrtsempfehlungen"

    if os.path.isdir(png_dir):
        for fname in os.listdir(png_dir):
            if not fname.lower().endswith(".png"):
                continue
            local_path = os.path.join(png_dir, fname)
            try:
                # Assumes common has a helper for FTP uploads (see below).
                common.upload_file_to_ftp(local_path, remote_png_dir)
                logging.info("Uploaded PNG %s to FTP folder %s", fname, remote_png_dir)
            except Exception as e:
                logging.exception("Failed to upload %s: %s", local_path, e)
    else:
        logging.warning("PNG directory does not exist: %s", png_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
