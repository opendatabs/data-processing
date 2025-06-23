import logging

import typer

from config import DATA_DIR, ODS_PLZ, ODS_STREETS, ODS_VIERTEL, OPENDATA_CSV, OUTPUT_CSV
from enrichment import add_availability_flags, annotate, reconcile_wohnviertel
from io_helpers import download_gwr, download_shapefile, read_pks
from viz import availability_histogram, distance_histogram, percentage_histogram

app = typer.Typer(add_completion=False)


@app.command()
def run(plot: bool = typer.Option(False, help="Write pre- and post-plots")):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    df_raw = read_pks()
    if plot:
        availability_histogram(add_availability_flags(df_raw.copy()), DATA_DIR / "availability_pre.png")

    gdf_viertel = download_shapefile(ODS_VIERTEL)
    gdf_streets = download_shapefile(ODS_STREETS)
    gdf_plz = download_shapefile(ODS_PLZ)
    gwr_df = download_gwr()

    df_enriched = annotate(df_raw, gdf_viertel, gdf_streets, gdf_plz, gwr_df)
    df_enriched.to_csv(OUTPUT_CSV, index=False)
    logging.info("Saved → %s", OUTPUT_CSV)

    # ---- Open-Data reconciliation ------------------------------------
    df_final = reconcile_wohnviertel(df_enriched.copy())
    df_final.to_csv(OPENDATA_CSV, index=False)
    logging.info("Open-data file → %s", "100449_stata_kriminalitaet.csv")

    if plot:
        availability_histogram(
            add_availability_flags(df_enriched.copy(), include_georef=True),
            DATA_DIR / "availability_post.png",
        )

        availability_histogram(
            add_availability_flags(
                df_enriched.copy(),
                include_georef=True,
                include_strasse_centroid=True,
            ),
            DATA_DIR / "availability_post_street.png",
        )

        distance_histogram(
            df_enriched,
            output=DATA_DIR / "distance_histogram.png",
        )
        logging.info("Plots written → availability_pre/post.png & distance_histogram.png")

        percentage_histogram(
            df_enriched,
            output=DATA_DIR / "percentage_histogram.png",
        )
        logging.info("Plots written → availability_pre/post, distance_histogram, percentage_histogram")


if __name__ == "__main__":
    app()
