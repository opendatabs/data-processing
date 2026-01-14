"""
Example script showing how to use calculate_hexagon_population to add population data to hexagons.

This can be used in stata_requisitionen or other projects that work with hexagons.
"""

import logging
from pathlib import Path

from etl import calculate_hexagon_population

logging.basicConfig(level=logging.INFO)


def main():
    # Example 1: Load hexagons from file and calculate population from dataset
    hexagon_file = Path("data_orig/hexagonalraster/hexaraster_kanton_100.shp")

    if hexagon_file.exists():
        hexagons_with_pop = calculate_hexagon_population(
            hexagons=hexagon_file,
            population_blocks=None,  # Will load from dataset 100062
            dataset_id="100062",
            year="2024",
            use_area_weighting=False,  # Use area-weighted approach for accuracy
        )

        # Save result
        output_path = Path("data/hexagons_with_population.geojson")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        hexagons_with_pop.to_file(output_path, driver="GeoJSON")
        logging.info(f"Saved hexagons with population to {output_path}")

        # Print summary
        print("\nSummary:")
        print(f"  Total hexagons: {len(hexagons_with_pop)}")
        print(f"  Total population: {hexagons_with_pop['population'].sum():,}")
        print(f"  Mean population per hexagon: {hexagons_with_pop['population'].mean():.1f}")
        print(f"  Hexagons with population > 0: {(hexagons_with_pop['population'] > 0).sum()}")
    else:
        logging.warning(f"Hexagon file not found: {hexagon_file}")
        logging.info("Example usage:")
        logging.info("  hexagons_with_pop = calculate_hexagon_population(")
        logging.info("      hexagons='path/to/hexagons.shp',")
        logging.info("      population_blocks=None,  # or path to blocks file")
        logging.info("      use_area_weighting=True")
        logging.info("  )")


if __name__ == "__main__":
    main()
