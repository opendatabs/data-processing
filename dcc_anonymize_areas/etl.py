import io, logging, requests
import geopandas as gpd
import networkx as nx
import numpy as np
from shapely.validation import make_valid
from shapely.geometry import Point
from shapely.ops import unary_union

THRESHOLD_M = 100         # polygon-to-polygon distance threshold (m)
GROUP_MIN = 50            # minimum people per group


def get_dataset(dataset_id: str) -> gpd.GeoDataFrame:
    url = f"https://data.bs.ch/explore/dataset/{dataset_id}/download/"
    params = {"format": "geojson", "refine.jahr": "2024"}
    r = requests.get(url, params=params); r.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(r.content))
    logging.info(f"Dataset {dataset_id}: {len(gdf)} rows, {len(gdf.columns)} cols.")
    return gdf


def to_metric_crs(gdf: gpd.GeoDataFrame, target="EPSG:2056") -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS.")
    return gdf.to_crs(target) if gdf.crs.is_geographic or str(gdf.crs).endswith("4326") else gdf


def _valid(geom):
    try:
        return make_valid(geom)
    except Exception:
        try:
            return geom.buffer(0)
        except Exception:
            c = geom.centroid
            return Point(c.x, c.y).buffer(0.01)


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
        groups_gdf = groups_gdf.to_crs("EPSG:2056")

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

        nbrs = list(GG.neighbors(g))
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


def build_graph(gdf: gpd.GeoDataFrame) -> tuple[nx.Graph, gpd.GeoDataFrame]:
    """
    Build a graph where nodes are blocks and edges connect blocks whose
    polygon distance ≤ THRESHOLD_M (meters). Uses GeoPandas sindex for
    robust cross-version spatial querying.
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
    # Ensure positional index 0..n-1 so sindex outputs are position-safe
    gdf = gdf.reset_index(drop=True)

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

    # Spatial index (rtree/pygeos/shapely-backed)
    sindex = gdf.sindex
    if sindex is None:
        logging.warning("No spatial index available; falling back to O(n^2) pairing.")
        candidate_pairs = [(i, j) for i in range(n) for j in range(i+1, n)]
    else:
        # Coarse candidate pairs: buffer each polygon by THRESHOLD_M and query bbox intersects
        buffers = gdf.geometry.buffer(THRESHOLD_M)
        candidate_pairs = set()
        # Try query_bulk with predicate if available; else per-feature query fallback
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
                    hits = sindex.query(buf)  # very old GeoPandas
                # hits is array-like of positional indices because we reset_index(drop=True)
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

def main():
    logging.basicConfig(level=logging.INFO)
    gdf = get_dataset("100062")
    keep = ["block","bez_id","wov_id","gesbev_f","geometry"]
    gdf = gdf[keep].copy()

    G, gdf_m = build_graph(gdf)
    grouped = group_blocks(G, gdf_m, min_sum=GROUP_MIN)
    grouped = enforce_minimum(grouped, G, min_sum=GROUP_MIN)

    n_ge50 = int((grouped["sum_gesbev_f"] >= GROUP_MIN).sum())
    logging.info(f"Final: {len(grouped)} areas; {n_ge50} meet ≥{GROUP_MIN}.")
    grouped.to_file("data/basel_anonymized_areas.geojson", driver="GeoJSON")

if __name__ == "__main__":
    main()
