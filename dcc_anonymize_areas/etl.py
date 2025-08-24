import os, io, logging, requests
import geopandas as gpd
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from shapely.validation import make_valid
from shapely.geometry import Point
from shapely.ops import unary_union

THRESHOLD_M = 100         # polygon-to-polygon distance threshold (m)
GROUP_MIN   = 50          # minimum people per group
YEAR_FILTER = "2024"      # dataset year filter
TARGET_CRS  = "EPSG:2056" # Swiss meters

# -------------------------------
# Data access & CRS utilities
# -------------------------------

def get_dataset(dataset_id: str) -> gpd.GeoDataFrame:
    url = f"https://data.bs.ch/explore/dataset/{dataset_id}/download/"
    params = {"format": "geojson", "refine.jahr": YEAR_FILTER}
    r = requests.get(url, params=params); r.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(r.content))
    logging.info(f"Dataset {dataset_id}: {len(gdf)} rows, {len(gdf.columns)} cols.")
    return gdf

def to_metric_crs(gdf: gpd.GeoDataFrame, target=TARGET_CRS) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS.")
    return gdf.to_crs(target) if gdf.crs.is_geographic or str(gdf.crs).endswith("4326") else gdf

# -------------------------------
# Geometry resilience
# -------------------------------

def _valid(geom):
    try:
        return make_valid(geom)
    except Exception:
        try:
            return geom.buffer(0)
        except Exception:
            c = geom.centroid
            return Point(c.x, c.y).buffer(0.01)

# -------------------------------
# Group graph construction
# -------------------------------

def _build_group_graph(groups_gdf: gpd.GeoDataFrame, G: nx.Graph) -> nx.Graph:
    # Map block -> group_id
    block_to_gid = {}
    for _, r in groups_gdf.iterrows():
        for b in r["blocks"]:
            block_to_gid[b] = int(r["group_id"])

    GG = nx.Graph()
    for _, r in groups_gdf.iterrows():
        gid = int(r["group_id"])
        GG.add_node(gid)

    # Add inter-group adjacency with min polygon-edge weight
    for u, v, edata in G.edges(data=True):
        gu, gv = block_to_gid[u], block_to_gid[v]
        if gu == gv:
            continue
        w = float(edata.get("weight", float("inf")))
        if GG.has_edge(gu, gv):
            if w < GG[gu][gv]["weight"]:
                GG[gu][gv]["weight"] = w
        else:
            GG.add_edge(gu, gv, weight=w)
    return GG

def enforce_minimum(groups_gdf: gpd.GeoDataFrame, G: nx.Graph, min_sum: int) -> gpd.GeoDataFrame:
    # Work in metric CRS
    groups_gdf = groups_gdf.copy()
    if groups_gdf.crs is None or "4326" in str(groups_gdf.crs):
        groups_gdf = groups_gdf.to_crs(TARGET_CRS)

    GG = _build_group_graph(groups_gdf, G)

    # Materialize group attributes for fast updates
    attrs = {}
    for _, r in groups_gdf.iterrows():
        gid = int(r["group_id"])
        attrs[gid] = {
            "blocks": list(r["blocks"]),
            "sum": int(r["sum_gesbev_f"]),
            "n_blocks": int(r["n_blocks"]),
            "bez": set(r["bez_ids"]),
            "wov": set(r["wov_ids"]),
            "geom": r.geometry
        }

    def centroid(gid): return attrs[gid]["geom"].centroid

    def tier(a, b):
        if attrs[a]["bez"] & attrs[b]["bez"]: return 0
        if attrs[a]["wov"] & attrs[b]["wov"]: return 1
        return 2

    # Merge loop
    while True:
        smalls = [g for g, a in attrs.items() if a["sum"] < min_sum]
        if not smalls:
            break

        # Process the smallest first (helps terminate quickly)
        g = min(smalls, key=lambda x: attrs[x]["sum"])

        nbrs = list(GG.neighbors(g)) if GG.has_node(g) else []
        if nbrs:
            # Prefer same bez_id, then wov_id, then shortest edge; tie-break by larger absorber
            nbrs.sort(key=lambda h: (tier(g, h), GG[g][h]["weight"], -attrs[h]["sum"]))
            h = nbrs[0]
        else:
            # Isolated component: attach to nearest centroid of any other group
            others = [k for k in attrs.keys() if k != g]
            c_g = centroid(g)
            h = min(others, key=lambda k: c_g.distance(centroid(k)))

        # Merge g -> h
        a, b = attrs[h], attrs[g]
        a["blocks"].extend(b["blocks"])
        a["sum"] += b["sum"]
        a["n_blocks"] += b["n_blocks"]
        a["bez"] |= b["bez"]
        a["wov"] |= b["wov"]
        a["geom"] = unary_union([a["geom"], b["geom"]])

        # Update group graph GG: redirect g's neighbors to h with min weights
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

        # Drop g
        del attrs[g]

    # Rebuild GeoDataFrame with fresh sequential IDs
    records = []
    for new_gid, (old_gid, a) in enumerate(attrs.items(), start=1):
        records.append({
            "group_id": new_gid,
            "n_blocks": a["n_blocks"],
            "sum_gesbev_f": int(a["sum"]),
            "bez_ids": sorted(a["bez"]),
            "wov_ids": sorted(a["wov"]),
            "blocks": a["blocks"],
            "geometry": a["geom"],
        })
    out = gpd.GeoDataFrame(records, geometry="geometry", crs=groups_gdf.crs)
    return out

# -------------------------------
# Graph building (blocks)
# -------------------------------

def build_graph(gdf: gpd.GeoDataFrame) -> tuple[nx.Graph, gpd.GeoDataFrame]:
    """
    Build a graph where nodes are blocks and edges connect blocks whose
    polygon distance ≤ THRESHOLD_M (meters). Uses GeoPandas sindex.
    """
    cols_needed = {"block","bez_id","wov_id","gesbev_f","geometry"}
    missing = cols_needed - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    gdf = gdf.loc[gdf.geometry.notna(), list(cols_needed)].copy()
    gdf["gesbev_f"] = gdf["gesbev_f"].fillna(0).astype(int)
    gdf["geometry"] = gdf["geometry"].apply(_valid)
    gdf = to_metric_crs(gdf)
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf.reset_index(drop=True)  # 0..n-1 for sindex safety

    n = len(gdf)
    G = nx.Graph()
    for _, row in gdf.iterrows():
        G.add_node(
            row["block"],
            weight=int(row["gesbev_f"]),
            bez_id=row["bez_id"],
            wov_id=row["wov_id"]
        )

    if n <= 1:
        logging.info("≤1 geometry; returning nodes-only graph.")
        return G, gdf

    # Spatial index
    sindex = gdf.sindex
    if sindex is None:
        logging.warning("No spatial index available; falling back to O(n^2) pairing.")
        candidate_pairs = [(i, j) for i in range(n) for j in range(i+1, n)]
    else:
        # Coarse candidate pairs: buffer each polygon by THRESHOLD_M and query bbox intersects
        buffers = gdf.geometry.buffer(THRESHOLD_M)
        candidate_pairs = set()
        try:
            src_idx, tgt_idx = sindex.query_bulk(buffers, predicate="intersects")
            for i, j in zip(src_idx.tolist(), tgt_idx.tolist()):
                if j > i:
                    candidate_pairs.add((int(i), int(j)))
        except Exception:
            for i, buf in enumerate(buffers):
                try:
                    hits = sindex.query(buf, predicate="intersects")
                except Exception:
                    hits = sindex.query(buf)
                for j in np.asarray(hits, dtype=int).tolist():
                    if j > i:
                        candidate_pairs.add((i, j))
        candidate_pairs = list(candidate_pairs)

    # Prepare arrays for centroid distance
    centsX = gdf["centroid"].x.to_numpy()
    centsY = gdf["centroid"].y.to_numpy()
    blocks = gdf["block"].to_numpy()

    # Filter by exact polygon distance and add edges
    edges_added = 0
    for i, j in candidate_pairs:
        gi = gdf.geometry.iloc[i]
        gj = gdf.geometry.iloc[j]
        d_poly = float(gi.distance(gj))
        if d_poly <= THRESHOLD_M:
            dx = centsX[i] - centsX[j]
            dy = centsY[i] - centsY[j]
            d_cent = float(np.hypot(dx, dy))
            u, v = blocks[i], blocks[j]
            prev = G.get_edge_data(u, v)
            if (prev is None) or (d_poly < prev.get("weight", float("inf"))):
                G.add_edge(u, v, weight=d_poly, centroid_w=d_cent)
                edges_added += 1

    logging.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges (added {edges_added}).")
    return G, gdf

# -------------------------------
# Greedy contiguous grouping
# -------------------------------

def group_blocks(G: nx.Graph, gdf: gpd.GeoDataFrame, min_sum:int=GROUP_MIN):
    """
    Greedy contiguous grouping:
      - Start from heaviest unassigned block.
      - BFS add neighbors until sum ≥ min_sum.
      - Priority tiers: same bez_id → same wov_id → any.
    Returns: dissolved groups with metadata.
    """
    weight = {n: G.nodes[n]["weight"] for n in G.nodes}
    bez    = {n: G.nodes[n]["bez_id"] for n in G.nodes}
    wov    = {n: G.nodes[n]["wov_id"] for n in G.nodes}

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

        target_bez = bez[start]
        target_wov = wov[start]

        group = [start]
        total = weight[start]
        frontier = [start]
        visited = {start}

        def neighbor_candidates():
            cans = []
            for u in list(frontier):
                for v in G.neighbors(u):
                    if v in visited or v not in unassigned:
                        continue
                    if bez[v]==target_bez: tier = 0
                    elif wov[v]==target_wov: tier = 1
                    else: tier = 2
                    e = G[u][v].get("weight", float("inf"))  # polygon distance (m)
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
            group = [start]
            total = weight[start]
            visited = {start}
            frontier = [start]

            for allowed_tier in (0,1,2):
                while total < min_sum:
                    cans = []
                    for u in list(frontier):
                        for v in G.neighbors(u):
                            if v in visited or v not in unassigned:
                                continue
                            if bez[v]==target_bez: tier=0
                            elif wov[v]==target_wov: tier=1
                            else: tier=2
                            if tier>allowed_tier:
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
    records = []
    for gid, members in enumerate(groups, start=1):
        sub = by_block.loc[members]
        geom = unary_union(list(sub.geometry))   # cross-version safe dissolve
        total = int(sub["gesbev_f"].sum())
        bez_ids = list(sub["bez_id"].unique())
        wov_ids = list(sub["wov_id"].unique())
        records.append({
            "group_id": gid,
            "n_blocks": len(members),
            "sum_gesbev_f": total,
            "bez_ids": bez_ids,
            "wov_ids": wov_ids,
            "blocks": members,
            "geometry": geom
        })
    groups_gdf = gpd.GeoDataFrame(records, geometry="geometry", crs=gdf.crs)
    return groups_gdf

# -------------------------------
# Visualization helpers
# -------------------------------

def _ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def _categorical_colors(n: int):
    """
    Build a reproducible list of distinct-ish colors using matplotlib's tab20,
    repeated if needed.
    """
    base = plt.get_cmap("tab20").colors
    reps = int(np.ceil(n / len(base)))
    palette = list(base) * reps
    return palette[:n]

def plot_before_after(blocks_gdf: gpd.GeoDataFrame,
                      grouped_initial: gpd.GeoDataFrame,
                      grouped_final: gpd.GeoDataFrame,
                      outdir="figs",
                      prefix="basel"):
    _ensure_dirs(outdir)

    # Common extent (in metric CRS)
    blocks_gdf = to_metric_crs(blocks_gdf)
    grouped_initial = to_metric_crs(grouped_initial)
    grouped_final = to_metric_crs(grouped_final)

    xmin, ymin, xmax, ymax = blocks_gdf.total_bounds
    pad = 0.02 * max(xmax - xmin, ymax - ymin)
    extent = (xmin - pad, xmax + pad, ymin - pad, ymax + pad)

    # 1) Raw blocks (thin outlines), population shading optional
    fig, ax = plt.subplots(figsize=(10, 10))
    blocks_gdf.plot(ax=ax, edgecolor="black", facecolor="none", linewidth=0.3)
    ax.set_title("Baseline: Block outlines")
    ax.set_xlim(extent[0], extent[1]); ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_0_blocks.png"), dpi=200)
    plt.close(fig)

    # 2) Initial groups (after greedy grouping)
    fig, ax = plt.subplots(figsize=(10, 10))
    colors = _categorical_colors(len(grouped_initial))
    grouped_initial.sort_values("group_id").plot(ax=ax,
        color=colors, edgecolor="white", linewidth=0.5)
    blocks_gdf.boundary.plot(ax=ax, color="black", linewidth=0.2, alpha=0.4)
    ax.set_title(f"Initial groups (after greedy) — n={len(grouped_initial)}")
    ax.set_xlim(extent[0], extent[1]); ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_1_initial_groups.png"), dpi=200)
    plt.close(fig)

    # 3) Final groups (after enforce_minimum)
    fig, ax = plt.subplots(figsize=(10, 10))
    colors = _categorical_colors(len(grouped_final))
    grouped_final.sort_values("group_id").plot(ax=ax,
        color=colors, edgecolor="white", linewidth=0.5)
    blocks_gdf.boundary.plot(ax=ax, color="black", linewidth=0.2, alpha=0.4)
    n_ge50 = int((grouped_final["sum_gesbev_f"] >= GROUP_MIN).sum())
    ax.set_title(f"Final groups (all ≥ {GROUP_MIN}) — n={len(grouped_final)}, eligible={n_ge50}")
    ax.set_xlim(extent[0], extent[1]); ax.set_ylim(extent[2], extent[3])
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{prefix}_2_final_groups.png"), dpi=200)
    plt.close(fig)

# -------------------------------
# Orchestration
# -------------------------------

def main():
    logging.basicConfig(level=logging.INFO)
    _ensure_dirs("data", "figs")

    # 1) Load
    gdf = get_dataset("100062")
    keep = ["block","bez_id","wov_id","gesbev_f","geometry"]
    gdf = gdf[keep].copy()

    # 2) Graph build
    G, gdf_m = build_graph(gdf)

    # 3) Initial greedy grouping
    grouped_initial = group_blocks(G, gdf_m, min_sum=GROUP_MIN)

    # 4) Enforce minimum via merges
    grouped_final = enforce_minimum(grouped_initial, G, min_sum=GROUP_MIN)

    # 5) Log + write outputs
    n_ge50 = int((grouped_final["sum_gesbev_f"] >= GROUP_MIN).sum())
    logging.info(f"Final: {len(grouped_final)} areas; {n_ge50} meet ≥{GROUP_MIN}.")

    grouped_final.to_file("data/basel_anonymized_areas.geojson", driver="GeoJSON")
    grouped_initial.to_file("data/basel_initial_groups.geojson", driver="GeoJSON")

    # 6) Static before/after maps
    plot_before_after(
        blocks_gdf=gdf_m,
        grouped_initial=grouped_initial,
        grouped_final=grouped_final,
        outdir="figs",
        prefix="basel"
    )

if __name__ == "__main__":
    main()
