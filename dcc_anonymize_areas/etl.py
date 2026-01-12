# etl_common.py
import io
import logging
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import requests
from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.validation import make_valid

TARGET_CRS = "EPSG:2056"
YEAR_FILTER = "2024"


# ---------- I/O & CRS ----------
def get_dataset(dataset_id: str, year: str = YEAR_FILTER) -> gpd.GeoDataFrame:
    url = f"https://data.bs.ch/explore/dataset/{dataset_id}/download/"
    r = requests.get(url, params={"format": "geojson", "refine.jahr": year})
    r.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(r.content))
    logging.info(f"Dataset {dataset_id}: {len(gdf)} rows, {len(gdf.columns)} cols.")
    return gdf


def to_metric_crs(gdf: gpd.GeoDataFrame, target: str = TARGET_CRS) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS.")
    return gdf.to_crs(target) if gdf.crs.is_geographic or str(gdf.crs).endswith("4326") else gdf


# ---------- Hexagon population calculation ----------
def calculate_hexagon_population(
    hexagons: gpd.GeoDataFrame | str,
    population_blocks: gpd.GeoDataFrame | str | None = None,
    dataset_id: str = "100062",
    year: str = YEAR_FILTER,
    population_column: str = "gesbev_f",
    use_area_weighting: bool = True,
) -> gpd.GeoDataFrame:
    """
    Calculate population for each hexagon by intersecting with population blocks.
    
    Parameters:
    -----------
    hexagons : GeoDataFrame or str
        Hexagons to calculate population for. If str, treated as file path.
    population_blocks : GeoDataFrame, str, or None
        Population blocks with geometry and population data. If None, loads from dataset.
        If str, treated as file path.
    dataset_id : str
        Dataset ID to load population blocks from (default: "100062").
    year : str
        Year filter for dataset (default: YEAR_FILTER).
    population_column : str
        Column name containing population count (default: "gesbev_f").
    use_area_weighting : bool
        If True, uses area-weighted population for partial overlaps.
        If False, assigns full population to hexagon if block centroid is within.
    
    Returns:
    --------
    GeoDataFrame
        Hexagons with added 'population' column containing total population.
    """
    # Load hexagons
    if isinstance(hexagons, str):
        hex_gdf = gpd.read_file(hexagons)
        logging.info(f"Loaded hexagons from {hexagons}: {len(hex_gdf)} hexagons")
    else:
        hex_gdf = hexagons.copy()
    
    # Ensure CRS
    if hex_gdf.crs is None:
        hex_gdf = hex_gdf.set_crs(TARGET_CRS)
    else:
        hex_gdf = hex_gdf.to_crs(TARGET_CRS)
    
    # Load population blocks
    if population_blocks is None:
        blocks_gdf = get_dataset(dataset_id, year=year)
        if population_column not in blocks_gdf.columns:
            raise ValueError(f"Population column '{population_column}' not found in dataset")
    elif isinstance(population_blocks, str):
        blocks_gdf = gpd.read_file(population_blocks)
        logging.info(f"Loaded population blocks from {population_blocks}: {len(blocks_gdf)} blocks")
    else:
        blocks_gdf = population_blocks.copy()
    
    # Ensure population blocks have required columns
    if "geometry" not in blocks_gdf.columns:
        raise ValueError("Population blocks must have a 'geometry' column")
    if population_column not in blocks_gdf.columns:
        raise ValueError(f"Population column '{population_column}' not found in population blocks")
    
    # Ensure CRS for blocks
    if blocks_gdf.crs is None:
        blocks_gdf = blocks_gdf.set_crs(TARGET_CRS)
    else:
        blocks_gdf = blocks_gdf.to_crs(TARGET_CRS)
    
    # Ensure valid geometries
    blocks_gdf = blocks_gdf.copy()
    blocks_gdf["geometry"] = blocks_gdf["geometry"].apply(valid_geom)
    hex_gdf = hex_gdf.copy()
    hex_gdf["geometry"] = hex_gdf["geometry"].apply(valid_geom)
    
    # Calculate population per hexagon
    # Work with reset indices for easier position-based access
    hex_gdf_work = hex_gdf.reset_index(drop=True).copy()
    hex_gdf_work["population"] = 0.0
    
    if use_area_weighting:
        # Area-weighted approach: for each hexagon-block intersection,
        # add (intersection_area / block_area) * block_population
        blocks_gdf = blocks_gdf.copy()
        blocks_gdf["block_area"] = blocks_gdf.geometry.area
        blocks_gdf["block_pop"] = blocks_gdf[population_column].fillna(0).astype(float)
        blocks_gdf_work = blocks_gdf.reset_index(drop=True)
        
        # Perform spatial join to find intersections
        joined = gpd.sjoin(
            hex_gdf_work[["geometry"]], 
            blocks_gdf_work[["geometry", "block_area", "block_pop"]], 
            how="left", 
            predicate="intersects"
        )
        
        # Calculate intersection areas and weighted population
        pop_dict = {}
        for hex_pos, row in joined.iterrows():
            if pd.isna(row["index_right"]):
                continue
            
            block_pos = int(row["index_right"])
            
            hex_geom = hex_gdf_work.iloc[hex_pos].geometry
            block_geom = blocks_gdf_work.iloc[block_pos].geometry
            
            try:
                intersection = hex_geom.intersection(block_geom)
                if intersection.is_empty or intersection.area == 0:
                    continue
                
                intersection_area = intersection.area
                block_area = blocks_gdf_work.iloc[block_pos]["block_area"]
                block_pop = blocks_gdf_work.iloc[block_pos]["block_pop"]
                
                if block_area > 0:
                    weighted_pop = (intersection_area / block_area) * block_pop
                    pop_dict[hex_pos] = pop_dict.get(hex_pos, 0.0) + weighted_pop
            except Exception as e:
                logging.warning(f"Error calculating intersection for hexagon {hex_pos}, block {block_pos}: {e}")
                continue
        
        # Assign population to hexagons
        for hex_pos, pop in pop_dict.items():
            hex_gdf_work.iloc[hex_pos, hex_gdf_work.columns.get_loc("population")] = pop
        
        # Round to integers
        hex_gdf_work["population"] = hex_gdf_work["population"].round().astype(int)
        
        # Copy population back to original hex_gdf (preserving original index)
        hex_gdf["population"] = hex_gdf_work["population"].values
    else:
        # Simple approach: assign full population if block centroid is within hexagon
        blocks_gdf["centroid"] = blocks_gdf.geometry.centroid
        blocks_centroids = gpd.GeoDataFrame(
            blocks_gdf[[population_column]], 
            geometry=blocks_gdf["centroid"],
            crs=blocks_gdf.crs
        )
        
        joined = gpd.sjoin(
            hex_gdf_work[["geometry"]],
            blocks_centroids,
            how="left",
            predicate="contains"
        )
        
        # Sum population by hexagon
        if "index_right" in joined.columns and len(joined) > 0:
            pop_by_hex = joined.groupby(joined.index)[population_column].sum()
            hex_gdf_work.loc[pop_by_hex.index, "population"] = pop_by_hex.values
        
        # Copy population back to original hex_gdf (preserving original index)
        hex_gdf["population"] = hex_gdf_work["population"].values
    
    hex_gdf["population"] = hex_gdf["population"].fillna(0).astype(int)
    logging.info(f"Calculated population for {len(hex_gdf)} hexagons. "
                 f"Total population: {hex_gdf['population'].sum()}, "
                 f"Mean per hexagon: {hex_gdf['population'].mean():.1f}")
    
    return hex_gdf


# ---------- Geometry resilience ----------
def valid_geom(geom):
    try:
        return make_valid(geom)
    except Exception:
        try:
            return geom.buffer(0)
        except Exception:
            c = geom.centroid
            return Point(c.x, c.y).buffer(0.01)


# ---------- Group graph from block-graph ----------
def build_group_graph(groups_gdf: gpd.GeoDataFrame, G_blocks: nx.Graph, weight_key: str = "weight") -> nx.Graph:
    block2gid = {b: int(r["group_id"]) for _, r in groups_gdf.iterrows() for b in r["blocks"]}
    GG = nx.Graph()
    for gid in groups_gdf["group_id"].astype(int):
        GG.add_node(gid)

    for u, v, edata in G_blocks.edges(data=True):
        gu, gv = block2gid[u], block2gid[v]
        if gu == gv:
            continue
        w = float(edata.get(weight_key, float("inf")))
        if GG.has_edge(gu, gv):
            if w < GG[gu][gv]["weight"]:
                GG[gu][gv]["weight"] = w
        else:
            GG.add_edge(gu, gv, weight=w)
    return GG


# ---------- Enforce minimum (merge) ----------
def enforce_minimum(
    groups_gdf: gpd.GeoDataFrame, G_blocks: nx.Graph, min_sum: int, tier_fn=None, weight_key: str = "weight"
) -> gpd.GeoDataFrame:
    groups_gdf = (
        groups_gdf.to_crs(TARGET_CRS) if (groups_gdf.crs is None or "4326" in str(groups_gdf.crs)) else groups_gdf
    )
    GG = build_group_graph(groups_gdf, G_blocks, weight_key=weight_key)

    attrs = {}
    for _, r in groups_gdf.iterrows():
        gid = int(r["group_id"])
        attrs[gid] = {
            "blocks": list(r["blocks"]),
            "sum": int(r["sum_gesbev_f"]),
            "n_blocks": int(r["n_blocks"]),
            "bez": set(r["bez_names"]),
            "wov": set(r["wov_names"]),
            "geom": r.geometry,
        }

    def default_tier(a, b):
        if attrs[a]["bez"] & attrs[b]["bez"]:
            return 0
        if attrs[a]["wov"] & attrs[b]["wov"]:
            return 1
        return 2

    tier = tier_fn or default_tier

    def centroid(gid):
        return attrs[gid]["geom"].centroid

    while True:
        smalls = [g for g, a in attrs.items() if a["sum"] < min_sum]
        if not smalls:
            break
        g = min(smalls, key=lambda x: attrs[x]["sum"])

        nbrs = list(GG.neighbors(g)) if GG.has_node(g) else []
        if nbrs:
            nbrs.sort(key=lambda h: (tier(g, h), GG[g][h]["weight"], -attrs[h]["sum"]))
            h = nbrs[0]
        else:
            others = [k for k in attrs if k != g]
            c_g = centroid(g)
            h = min(others, key=lambda k: c_g.distance(centroid(k)))

        # merge g -> h
        a, b = attrs[h], attrs[g]
        a["blocks"].extend(b["blocks"])
        a["sum"] += b["sum"]
        a["n_blocks"] += b["n_blocks"]
        a["bez"] |= b["bez"]
        a["wov"] |= b["wov"]
        a["geom"] = unary_union([a["geom"], b["geom"]])

        if GG.has_node(g):
            for k in list(GG.neighbors(g)):
                if k == h:
                    continue
                w = GG[g][k]["weight"]
                if GG.has_edge(h, k):
                    if w < GG[h][k]["weight"]:
                        GG[h][k]["weight"] = w
                else:
                    GG.add_edge(h, k, weight=w)
            GG.remove_node(g)
        del attrs[g]

    recs = []
    for new_gid, (_, a) in enumerate(attrs.items(), start=1):
        recs.append(
            {
                "group_id": new_gid,
                "n_blocks": a["n_blocks"],
                "sum_gesbev_f": int(a["sum"]),
                "bez_names": sorted(a["bez"]),
                "wov_names": sorted(a["wov"]),
                "blocks": a["blocks"],
                "geometry": a["geom"],
            }
        )
    return gpd.GeoDataFrame(recs, geometry="geometry", crs=groups_gdf.crs)


# ---------- Greedy grouping over any block-graph ----------
def group_blocks(G: nx.Graph, gdf: gpd.GeoDataFrame, min_sum: int, edge_weight_key: str = "weight"):
    weight = {n: G.nodes[n]["weight"] for n in G.nodes}
    bez = {n: G.nodes[n]["bez_name"] for n in G.nodes}
    wov = {n: G.nodes[n]["wov_name"] for n in G.nodes}

    unassigned = set(G.nodes)
    order = sorted(unassigned, key=lambda n: weight[n], reverse=True)
    groups = []

    for start in order:
        if start not in unassigned:
            continue
        if weight[start] >= min_sum:
            groups.append([start])
            unassigned.remove(start)
            continue

        target_bez, target_wov = bez[start], wov[start]
        group, total = [start], weight[start]
        frontier, visited = [start], {start}

        def neighbor_candidates():
            cans = []
            for u in list(frontier):
                for v in G.neighbors(u):
                    if v in visited or v not in unassigned:
                        continue
                    tier = 0 if bez[v] == target_bez else (1 if wov[v] == target_wov else 2)
                    e = G[u][v].get(edge_weight_key, float("inf"))
                    cans.append((tier, e, -weight[v], v))
            cans.sort()
            return [v for *_, v in cans]

        while total < min_sum:
            cands = neighbor_candidates()
            if not cands:
                break
            picked = cands[0]
            visited.add(picked)
            frontier.append(picked)
            group.append(picked)
            total += weight[picked]
            unassigned.remove(picked)

        if total < min_sum:
            for v in group[1:]:
                unassigned.add(v)
            group, total, visited, frontier = [start], weight[start], {start}, [start]
            for allowed in (0, 1, 2):
                while total < min_sum:
                    cans = []
                    for u in list(frontier):
                        for v in G.neighbors(u):
                            if v in visited or v not in unassigned:
                                continue
                            tier = 0 if bez[v] == target_bez else (1 if wov[v] == target_wov else 2)
                            if tier > allowed:
                                continue
                            cans.append((-weight[v], v))
                    if not cans:
                        break
                    cans.sort()
                    _, picked = cans[0]
                    visited.add(picked)
                    frontier.append(picked)
                    group.append(picked)
                    total += weight[picked]
                    unassigned.remove(picked)
                if total >= min_sum:
                    break

        groups.append(group)
        unassigned.discard(start)

    for n in list(unassigned):
        groups.append([n])
        unassigned.remove(n)

    by_block = gdf.set_index("block")
    recs = []
    for gid, members in enumerate(groups, start=1):
        sub = by_block.loc[members]
        recs.append(
            {
                "group_id": gid,
                "n_blocks": len(members),
                "sum_gesbev_f": int(sub["gesbev_f"].sum()),
                "bez_names": list(sub["bez_name"].unique()),
                "wov_names": list(sub["wov_name"].unique()),
                "blocks": members,
                "geometry": unary_union(list(sub.geometry)),
            }
        )
    return gpd.GeoDataFrame(recs, geometry="geometry", crs=gdf.crs)


# ---------- Viz ----------
def ensure_dirs(*paths):
    [os.makedirs(p, exist_ok=True) for p in paths]


def categorical_colors(n: int):
    base = list(plt.get_cmap("tab20").colors)  # 20-tuple → list
    if n <= 0:
        return []
    reps = (n + len(base) - 1) // len(base)  # ceil(n / len(base)) without floats
    return (base * reps)[:n]  # repeat list, then slice


def plot_before_after(
    blocks_gdf, grouped_initial, grouped_final, outdir="figs", prefix="basel", title_init="Initial", title_final="Final"
):
    ensure_dirs(outdir)
    blocks_gdf = to_metric_crs(blocks_gdf)
    grouped_initial = to_metric_crs(grouped_initial)
    grouped_final = to_metric_crs(grouped_final)

    xmin, ymin, xmax, ymax = blocks_gdf.total_bounds
    pad = 0.02 * max(xmax - xmin, ymax - ymin)
    extent = (xmin - pad, xmax + pad, ymin - pad, ymax + pad)

    fig, ax = plt.subplots(figsize=(10, 10))
    blocks_gdf.plot(ax=ax, edgecolor="black", facecolor="none", linewidth=0.3)
    ax.set_title("Blocks")
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_0_blocks.png"), dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 10))
    grouped_initial.sort_values("group_id").plot(
        ax=ax, color=categorical_colors(len(grouped_initial)), edgecolor="white", linewidth=0.5
    )
    blocks_gdf.boundary.plot(ax=ax, color="black", linewidth=0.2, alpha=0.4)
    ax.set_title(title_init)
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_1_initial.png"), dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 10))
    grouped_final.sort_values("group_id").plot(
        ax=ax, color=categorical_colors(len(grouped_final)), edgecolor="white", linewidth=0.5
    )
    blocks_gdf.boundary.plot(ax=ax, color="black", linewidth=0.2, alpha=0.4)
    ax.set_title(title_final)
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_2_final.png"), dpi=200)
    plt.close(fig)
