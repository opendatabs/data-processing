import io, logging, requests
import geopandas as gpd
import networkx as nx
import numpy as np
from shapely.validation import make_valid
from shapely.geometry import Point
from scipy.spatial import Delaunay
from scipy.spatial import QhullError

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
    try: return make_valid(geom)
    except Exception:
        try: return geom.buffer(0)
        except Exception:
            c = geom.centroid; return Point(c.x, c.y).buffer(0.01)

def _delaunay_edges(coords_unique: np.ndarray) -> set[tuple[int,int]]:
    tri = Delaunay(coords_unique)
    edges = set()
    for a,b,c in tri.simplices:
        edges.update({tuple(sorted((a,b))), tuple(sorted((b,c))), tuple(sorted((c,a)))})
    return edges

def build_graph(gdf: gpd.GeoDataFrame) -> nx.Graph:
    cols_needed = {"block","bez_id","wov_id","gesbev_f","geometry"}
    missing = cols_needed - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    gdf = gdf.loc[gdf.geometry.notna(), list(cols_needed)].copy()
    gdf["gesbev_f"] = gdf["gesbev_f"].fillna(0).astype(int)
    gdf["geometry"] = gdf["geometry"].apply(_valid)
    gdf = to_metric_crs(gdf)
    gdf["centroid"] = gdf.geometry.centroid

    # coords for Delaunay
    coords = np.column_stack((gdf["centroid"].x.values, gdf["centroid"].y.values))
    coords_unique, inverse = np.unique(coords, axis=0, return_inverse=True)

    G = nx.Graph()
    for _, row in gdf.iterrows():
        G.add_node(
            row["block"],
            weight=int(row["gesbev_f"]),
            bez_id=row["bez_id"],
            wov_id=row["wov_id"]
        )

    if len(coords_unique) < 3:
        logging.info("Not enough unique points for Delaunay; no edges created.")
        return G, gdf

    try:
        uniq_edges = _delaunay_edges(coords_unique)
    except QhullError:
        logging.info("Delaunay failed; returning nodes-only graph.")
        return G, gdf

    # Expand to original indices (handles duplicate centroids)
    cents  = gdf["centroid"].to_numpy()
    blocks = gdf["block"].to_numpy()

    for ui, uj in uniq_edges:
        group_i = np.where(inverse == ui)[0]
        group_j = np.where(inverse == uj)[0]
        for ii in group_i:
            for jj in group_j:
                if ii == jj:
                    continue
                dist_c = float(cents[ii].distance(cents[jj]))
                # if duplicates create multiple combos, keep the shortest
                prev = G.get_edge_data(blocks[ii], blocks[jj])
                if (prev is None) or (dist_c < prev.get("weight", float("inf"))):
                    G.add_edge(blocks[ii], blocks[jj], weight=dist_c)

    logging.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
    return G, gdf

def group_blocks(G: nx.Graph, gdf: gpd.GeoDataFrame, min_sum:int=GROUP_MIN):
    """
    Greedy contiguous grouping:
      - Start from heaviest unassigned block.
      - BFS add neighbors until sum ≥ min_sum.
      - Priority tiers for neighbors: same bez_id → same wov_id → any.
      - Minimizes groups by covering heavy nodes first.
    Returns: list of groups (list of block ids).
    """
    # quick lookup
    weight = {n: G.nodes[n]["weight"] for n in G.nodes}
    bez    = {n: G.nodes[n]["bez_id"] for n in G.nodes}
    wov    = {n: G.nodes[n]["wov_id"] for n in G.nodes}

    unassigned = set(G.nodes)
    # sort by weight desc
    order = sorted(unassigned, key=lambda n: weight[n], reverse=True)

    groups = []
    for start in order:
        if start not in unassigned:
            continue
        # single node already ≥ min_sum → its own group
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
                    e = G[u][v].get("weight", float("inf"))  # centroid distance (m)
                    cans.append((tier, e, -weight[v], v))
            # pick best tier → shortest edge → heaviest node
            cans.sort()
            return [v for *_, v in cans]


        # expand until threshold or no candidates
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
            # If still below, we may need to allow revisiting candidates not yet removed
            # Undo partial assignment (except start), then do a wider BFS with tiering
            for v in group[1:]:
                unassigned.add(v)
            group = [start]
            total = weight[start]
            visited = {start}
            frontier = [start]

            # broaden search progressively: first same bez, then same wov, then any
            for allowed_tier in (0,1,2):
                while total < min_sum:
                    # gather candidates restricted by allowed_tier
                    cans = []
                    for u in list(frontier):
                        for v in G.neighbors(u):
                            if v in visited or v not in unassigned:
                                continue
                            # compute tier
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

        # final: accept group (even if < min_sum, rare when isolated—document it)
        groups.append(group)
        # ensure start removed
        unassigned.discard(start)

    # Any leftovers (shouldn’t happen): each as singleton
    for n in list(unassigned):
        groups.append([n])
        unassigned.remove(n)

    # Build dissolved polygons + metadata
    by_block = gdf.set_index("block")
    records = []
    for gid, members in enumerate(groups, start=1):
        sub = by_block.loc[members]
        geom = sub.geometry.union_all()
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
    # keep only needed cols
    keep = ["block","bez_id","wov_id","gesbev_f","geometry"]
    gdf = gdf[keep].copy()

    G, gdf_m = build_graph(gdf)
    grouped = group_blocks(G, gdf_m, min_sum=GROUP_MIN)

    # Inspect
    n_ge50 = int((grouped["sum_gesbev_f"] >= GROUP_MIN).sum())
    logging.info(f"Built {len(grouped)} areas; {n_ge50} meet ≥{GROUP_MIN}.")
    grouped.to_file("data/basel_anonymized_areas.geojson", driver="GeoJSON")

if __name__ == "__main__":
    main()
