import os
import polars as pl
from kapo_ordnungsbussen import credentials


def group_by_station(df):
    # Step 1: Filter rows for BuZi and Übertretungsjahr
    filtered_df = df.filter(
        (df["BuZi"].str.starts_with("303")) & (df["Übertretungsjahr"] == 2023)
    )

    # Step 2: Concatenate "GPS Länge", "GPS Breite" to create a new column "GPS Koordinaten"
    filtered_df = filtered_df.with_columns(
        pl.concat_str([filtered_df["GPS Breite"], pl.lit(", "), filtered_df["GPS Länge"]]).alias("GPS Koordinaten")
    )
    # Step 3: Extract coordinates from the "coordinates" column. If they are enclosed in () or in [], remove them
    filtered_df = filtered_df.with_columns(filtered_df["coordinates"].alias("Nominatim Koordinaten"))
    filtered_df = filtered_df.with_columns(
        filtered_df["Nominatim Koordinaten"]
        .str.replace(r"\[", "")
        .str.replace(r"\]", "")
        .str.replace(r"\(", "")
        .str.replace(r"\)", "")
        .alias("Nominatim Koordinaten")
    )
    # If GPS Länge and GPS Breite are not available, use Nominatim Koordinaten
    filtered_df = filtered_df.with_columns(
        pl.when(filtered_df["GPS Koordinaten"].is_null())
        .then(filtered_df["Nominatim Koordinaten"])
        .otherwise(filtered_df["GPS Koordinaten"])
        .alias("GPS Koordinaten")
    )

    # Step 4: Group by address, GPS Länge, and GPS Breite and aggregate
    result = (
        filtered_df.group_by(["Ü-Ort STR", "Ü-Ort STR-NR", "Ü-Ort PLZ", "Ü-Ort ORT",
                              "BuZi Zus.","BuZi Ver.", "GPS Koordinaten"])
        .agg([
            pl.len().alias("Anzahl Übertretungen"),
            pl.sum("Bussen-Betrag").alias("Ertrag aus Erfassung")
        ])
    )

    # Step 5: Sort the result if needed
    result_sorted = result.sort("Ertrag aus Erfassung", descending=True)

    # Save or display the result
    output_path = os.path.join(credentials.export_path, "filtered_and_grouped_results_2023.csv")
    result_sorted.write_csv(output_path)


if __name__ == '__main__':
    # Specify schema overrides to treat the problematic column as a string
    schema = {
        "Ü-Ort STR-NR": pl.Utf8,  # Treat this column as strings
        "Bussen-Betrag": pl.Float64,  # Ensure numeric values for aggregation
        "Übertretungsjahr": pl.Int64,  # Year as integer
        # Add other overrides as needed
    }
    # Load the data
    file_path = os.path.join(credentials.export_path, 'Ordnungsbussen_OGD_all.csv')
    df = pl.read_csv(file_path, schema_overrides=schema)
    # Group the data by the station ID
    group_by_station(df)
