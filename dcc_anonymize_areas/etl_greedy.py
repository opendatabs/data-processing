# etl_threshold.py
import logging
from etl import get_dataset, ensure_dirs, plot_before_after, group_blocks, enforce_minimum
from builders import build_graph_threshold

GROUP_MIN = 50
DATASET = "100062"
THRESHOLD_M = 100

def main():
    logging.basicConfig(level=logging.INFO)
    ensure_dirs("data","figs")

    gdf = get_dataset(DATASET)[["block","bez_id","wov_id","gesbev_f","geometry"]].copy()
    G, gdf_m = build_graph_threshold(gdf, THRESHOLD_M)

    grouped_initial = group_blocks(G, gdf_m, min_sum=GROUP_MIN, edge_weight_key="weight")
    grouped_final   = enforce_minimum(grouped_initial, G, min_sum=GROUP_MIN, weight_key="weight")

    grouped_final.to_file("data/basel_greedy_anonymized_areas.geojson", driver="GeoJSON")
    grouped_initial.to_file("data/basel_greedy_initial_groups.geojson", driver="GeoJSON")
    plot_before_after(gdf_m, grouped_initial, grouped_final, outdir="figs", prefix="basel_greedy",
                      title_init="Initial (greedy)", title_final=f"Final (≥{GROUP_MIN})")

if __name__ == "__main__": main()
