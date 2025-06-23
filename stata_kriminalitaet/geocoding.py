from __future__ import annotations

import logging
import time

import geopandas as gpd
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from rapidfuzz import process
from shapely.geometry import Point

from config import ADDR_CACHE
from io_helpers import load_cache, save_cache


class Geocoder:
    """
    Hybrid geocoder:
    1. Exact match via GWR 'Gebäudeeingänge' (local authoritative DB).
    2. Fallback to Nominatim with caching and fuzzy-street repair.
    """

    def __init__(self, gwr_df: pd.DataFrame, bs_boundary: gpd.GeoDataFrame):
        self.gwr_lookup = self._prepare_gwr(gwr_df)
        self.bs_boundary = bs_boundary.unary_union
        self.nominatim = RateLimiter(Nominatim(user_agent="pks_geocoder").geocode, min_delay_seconds=1)
        self.cache: dict[str, tuple[float, float] | None] = load_cache(ADDR_CACHE)

    # ────────────────────────────────────────────────────────────────────── #
    # public
    # ────────────────────────────────────────────────────────────────────── #

    def coordinates(self, address: str) -> tuple[float, float] | None:
        """Return (lat, lon) if successful, else None."""
        if address in self.cache:
            return self.cache[address]

        # 1 – exact match in GWR
        if address in self.gwr_lookup:
            self._write_cache(address, self.gwr_lookup[address])
            return self.gwr_lookup[address]

        # 2 – Nominatim, first literal, then fuzzy
        candidate = self._query_nominatim(address)
        if candidate:
            self._write_cache(address, candidate)
            return candidate

        fuzzy = self._fuzzy_repair(address)
        candidate = self._query_nominatim(fuzzy) if fuzzy else None
        self._write_cache(address, candidate)
        return candidate

    # ────────────────────────────────────────────────────────────────────── #
    # internal helpers
    # ────────────────────────────────────────────────────────────────────── #

    @staticmethod
    def _prepare_gwr(df: pd.DataFrame) -> dict[str, tuple[float, float]]:
        df["address"] = df["strname"].str.strip() + " " + df["deinr"].astype(str) + ", " + df["dplzname"]
        return {
            addr: tuple(map(float, coords.split(",")))
            for addr, coords in df[["address", "eingang_koordinaten"]].dropna().itertuples(index=False)
        }

    def _query_nominatim(self, address: str | None) -> tuple[float, float] | None:
        if not address:
            return None
        try:
            loc = self.nominatim(address)
            if loc:
                point = Point(loc.longitude, loc.latitude)
                if self.bs_boundary.contains(point):
                    return (loc.latitude, loc.longitude)
            return None
        except Exception as exc:
            logging.warning("Nominatim fail for %s: %s", address, exc)
            time.sleep(5)
            return None

    def _fuzzy_repair(self, address: str) -> str | None:
        try:
            street, hnr, *rest = address.split(",", maxsplit=2)[0].split(" ")
            closest, _, _ = process.extractOne(street, self.gwr_lookup.keys())
            return f"{closest} {hnr}, {' '.join(rest)}"
        except Exception:
            return None

    def _write_cache(self, address: str, result: tuple[float, float] | None) -> None:
        self.cache[address] = result
        save_cache(ADDR_CACHE, self.cache)
