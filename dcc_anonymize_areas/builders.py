# builders.py (append this)
import logging, numpy as np, networkx as nx
import geopandas as gpd
from shapely.geometry import Point
from shapely.strtree import STRtree
from etl import to_metric_crs, valid_geom

def _prep_blocks(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cols = {"block","bez_id","wov_id","gesbev_f","geometry"}
    missing = cols - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing: {missing}")
    gdf = gdf.loc[gdf.geometry.notna(), list(cols)].copy()
    gdf["gesbev_f"] = gdf["gesbev_f"].fillna(0).astype(int)
    gdf["geometry"] = gdf["geometry"].apply(valid_geom)
    gdf = to_metric_crs(gdf)
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf.reset_index(drop=True)
    return gdf

def _bbox_diag(gdf: gpd.GeoDataFrame) -> float:
    xmin, ymin, xmax, ymax = gdf.total_bounds
    return float(np.hypot(xmax - xmin, ymax - ymin))

def build_graph_strtree(gdf: gpd.GeoDataFrame, k_neighbors: int = 6) -> tuple[nx.Graph, gpd.GeoDataFrame]:
    gdf = _prep_blocks(gdf)
    n = len(gdf)

    G = nx.Graph()
    for _, r in gdf.iterrows():
        G.add_node(r["block"], weight=int(r["gesbev_f"]), bez_id=r["bez_id"], wov_id=r["wov_id"])
    if n <= 1:
        logging.info("≤1 geometry; returning nodes-only graph.")
        return G, gdf

    # Centroids + tree
    centroids = list(gdf["centroid"])
    tree = STRtree(centroids)

    # Map for geometry→row index when query returns geometries
    wkb_to_idx = {c.wkb: i for i, c in enumerate(centroids)}

    # Heuristic radius ramp
    xmin, ymin, xmax, ymax = gdf.total_bounds
    diag = float(np.hypot(xmax - xmin, ymax - ymin))
    base_r = max(1.0, diag / 500.0)

    def to_index(cand):
        # cand may be an int, np.int64, or a geometry
        if isinstance(cand, (int, np.integer)):
            return int(cand)
        key = getattr(cand, "wkb", None)
        if key in wkb_to_idx:
            return wkb_to_idx[key]
        # rare fallback: map by nearest centroid
        return int(np.argmin([cand.distance(c) for c in centroids]))

    def k_nearest_indices(i: int) -> list[int]:
        p = centroids[i]
        r = base_r
        idxs = []
        for _ in range(12):  # expand radius until we get enough
            try:
                candidates = tree.query(p.buffer(r))          # Shapely 2.x geometries
            except TypeError:
                minx, miny, maxx, maxy = p.buffer(r).bounds   # Shapely 1.8 indices
                candidates = tree.query((minx, miny, maxx, maxy))
            idxs = [to_index(c) for c in candidates if to_index(c) != i]
            if len(idxs) >= k_neighbors:
                break
            r *= 2.0
        # sort by centroid distance; keep k
        px, py = p.x, p.y
        idxs = sorted(set(idxs), key=lambda j: (centroids[j].x - px)**2 + (centroids[j].y - py)**2)
        return idxs[:k_neighbors]

    # Wire edges (keep lightest if duplicate)
    by_block = gdf.set_index("block")
    edges_added = 0
    for i in range(n):
        ui_block = gdf.at[i, "block"]
        ui_geom = gdf.at[i, "geometry"]
        ui_cent = centroids[i]
        for j in k_nearest_indices(i):
            vj_block = gdf.at[j, "block"]
            if ui_block == vj_block:
                continue
            dx, dy = ui_cent.x - centroids[j].x, ui_cent.y - centroids[j].y
            d_cent = float(np.hypot(dx, dy))
            d_poly = float(ui_geom.distance(gdf.at[j, "geometry"]))
            u, v = (ui_block, vj_block) if ui_block < vj_block else (vj_block, ui_block)
            prev = G.get_edge_data(u, v)
            if (prev is None) or (d_cent < prev.get("weight", float("inf"))):
                G.add_edge(u, v, weight=d_cent, centroid_w=d_cent, poly_w=d_poly)
                edges_added += 1

    logging.info(f"STRtree kNN graph (k={k_neighbors}): {G.number_of_nodes()} nodes, {G.number_of_edges()} edges (added {edges_added}).")
    return G, gdf


def build_graph_threshold(gdf: gpd.GeoDataFrame, threshold_m: float) -> tuple[nx.Graph, gpd.GeoDataFrame]:
    cols = {"block","bez_id","wov_id","gesbev_f","geometry"}
    if cols - set(gdf.columns): raise ValueError(f"Missing: {cols - set(gdf.columns)}")
    gdf = gdf.loc[gdf.geometry.notna(), list(cols)].copy()
    gdf["gesbev_f"] = gdf["gesbev_f"].fillna(0).astype(int)
    gdf["geometry"] = gdf["geometry"].apply(valid_geom)
    gdf = to_metric_crs(gdf)
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf.reset_index(drop=True)

    G = nx.Graph()
    for _, r in gdf.iterrows():
        G.add_node(r["block"], weight=int(r["gesbev_f"]), bez_id=r["bez_id"], wov_id=r["wov_id"])
    if len(gdf) <= 1: return G, gdf

    sindex = gdf.sindex
    buffers = gdf.geometry.buffer(threshold_m)
    pairs = set()
    if sindex:
        try:
            i, j = sindex.query_bulk(buffers, predicate="intersects")
            for a, b in zip(i.tolist(), j.tolist()):
                if b > a: pairs.add((int(a), int(b)))
        except Exception:
            for a, buf in enumerate(buffers):
                hits = sindex.query(buf)  # fallback
                for b in map(int, hits):
                    if b > a: pairs.add((a, b))
    else:
        pairs = {(i,j) for i in range(len(gdf)) for j in range(i+1,len(gdf))}

    cx, cy = gdf["centroid"].x.to_numpy(), gdf["centroid"].y.to_numpy()
    blocks = gdf["block"].to_numpy()
    for i, j in pairs:
        gi, gj = gdf.geometry.iloc[i], gdf.geometry.iloc[j]
        d_poly = float(gi.distance(gj))
        if d_poly <= threshold_m:
            dx, dy = cx[i]-cx[j], cy[i]-cy[j]
            d_cent = float(np.hypot(dx, dy))
            u, v = blocks[i], blocks[j]
            prev = G.get_edge_data(u, v)
            if (prev is None) or (d_poly < prev.get("weight", float("inf"))):
                G.add_edge(u, v, weight=d_poly, centroid_w=d_cent)
    logging.info(f"Threshold graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
    return G, gdf

def _q(pt, ndigits=6): return (round(pt.x, ndigits), round(pt.y, ndigits))

def build_graph_delaunay(gdf: gpd.GeoDataFrame) -> tuple[nx.Graph, gpd.GeoDataFrame]:
    cols = {"block","bez_id","wov_id","gesbev_f","geometry"}
    if cols - set(gdf.columns): raise ValueError(f"Missing: {cols - set(gdf.columns)}")
    gdf = gdf.loc[gdf.geometry.notna(), list(cols)].copy()
    gdf["gesbev_f"] = gdf["gesbev_f"].fillna(0).astype(int)
    gdf["geometry"] = gdf["geometry"].apply(valid_geom)
    gdf = to_metric_crs(gdf)
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf.reset_index(drop=True)

    G = nx.Graph()
    for _, r in gdf.iterrows():
        G.add_node(r["block"], weight=int(r["gesbev_f"]), bez_id=r["bez_id"], wov_id=r["wov_id"])
    if len(gdf) <= 1: return G, gdf

    coord2blocks = {}
    for _, r in gdf.iterrows(): coord2blocks.setdefault(_q(r["centroid"]), []).append(r["block"])
    mp = MultiPoint([Point(x, y) for (x, y) in coord2blocks.keys()])
    tris = triangulate(mp)

    coord_edges = set()
    for tri in tris:
        xs, ys = tri.exterior.coords.xy
        verts = [(xs[i], ys[i]) for i in range(len(xs)-1)]
        for k in range(3):
            a = _q(Point(verts[k])); b = _q(Point(verts[(k+1)%3]))
            if a == b: continue
            u, v = (a, b) if a < b else (b, a)
            coord_edges.add((u, v))

    by_block = gdf.set_index("block")
    cent_lookup = { _q(c): c for c in gdf["centroid"] }

    for ca, cb in coord_edges:
        ba, bb = coord2blocks.get(ca, []), coord2blocks.get(cb, [])
        if not ba or not bb: continue
        pa, pb = cent_lookup[ca], cent_lookup[cb]
        dx, dy = pa.x - pb.x, pa.y - pb.y
        d_cent = float(np.hypot(dx, dy))
        for u in ba:
            for v in bb:
                if u == v: continue
                gi, gj = by_block.loc[u, "geometry"], by_block.loc[v, "geometry"]
                d_poly = float(gi.distance(gj))
                a, b = (u, v) if u < v else (v, u)
                prev = G.get_edge_data(a, b)
                if (prev is None) or (d_cent < prev.get("weight", float("inf"))):
                    G.add_edge(a, b, weight=d_cent, centroid_w=d_cent, poly_w=d_poly)
    logging.info(f"Delaunay graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
    return G, gdf
