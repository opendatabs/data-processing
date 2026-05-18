"""Derived columns for Bohrkataster (BOHA) before publish."""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd

# STAC / Dataspot property names (PascalCase with underscores)
COL_GEPLANT = "geplante_Bohrung"
COL_ZUSTAND = "Zustand_der_Bohrung"
COL_GEOTHERM = "Geothermische_Bohrung"
COL_ROHR_DM = "Rohr_Durchmesser"
COL_HOEHE_FELS = "Hoehe_Felsoberkante"
COL_HOEHE_GWL = "Hoehe_Grundwasserspiegel"


def _flag_true(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "yes", "ja"})


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()

    geplant = _flag_true(out[COL_GEPLANT])
    kassiert = out[COL_ZUSTAND].astype(str).str.strip().str.lower().eq("kassiert")
    geotherm = _flag_true(out[COL_GEOTHERM])
    rohr = pd.to_numeric(out[COL_ROHR_DM], errors="coerce")

    out["art_der_bohrung"] = np.select(
        [
            geplant,
            kassiert,
            geotherm,
            rohr.eq(-99),
            rohr.notna() & rohr.lt(200),
            rohr.notna() & rohr.ge(200),
        ],
        [
            "geplante Bohrung",
            "Bohrungen verrohrt (kassiert)",
            "Erdwärmebohrungen",
            "Sondierbohrung",
            "Bohrungen verrohrt (ø < 200mm)",
            "Bohrungen verrohrt (ø >= 200mm)",
        ],
        default=None,
    )

    hoehe_fels = pd.to_numeric(out[COL_HOEHE_FELS], errors="coerce")
    hoehe_gwl = pd.to_numeric(out[COL_HOEHE_GWL], errors="coerce")
    flurabstand = hoehe_fels - hoehe_gwl
    out["flurabstand"] = flurabstand.where(flurabstand.gt(0) & hoehe_fels.notna() & hoehe_gwl.notna())

    out["grundwasserdaten"] = np.select(
        [hoehe_gwl.notna() & hoehe_gwl.ge(0), hoehe_gwl.isna()],
        ["1", "0"],
        default="0",
    )

    return out
