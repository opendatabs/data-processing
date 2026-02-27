import asyncio
import io
import os
import shutil
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import common
import geopandas as gpd
import httpx
import pandas as pd
import requests
from common import change_tracking as ct
from dcc_backend_common.logger import get_logger, init_logger
from dotenv import load_dotenv
from owslib.wfs import WebFeatureService

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_01")
FTP_PASS = os.getenv("FTP_PASS_01")
URL_WMS = "https://wms.geo.bs.ch/?SERVICE=wms&REQUEST=GetCapabilities"
URL_WFS = "https://wfs.geo.bs.ch/"
WFS_CONCURRENCY = 8
HTTP_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
LOG_WFS_REQUESTS = True
GEOCAT_NAMESPACE = {
    "che": "http://www.geocat.ch/2008/che",
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
}
WMS_NAMESPACE = {"wms": "http://www.opengis.net/wms"}

logger = get_logger(__name__)


@dataclass(slots=True)
class RuntimeMetrics:
    """Collect runtime durations and counters for ETL hotspots."""

    timings: dict[str, float] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)

    def add_timing(self, key: str, seconds: float) -> None:
        self.timings[key] = self.timings.get(key, 0.0) + seconds

    def inc(self, key: str, amount: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + amount

    def log_summary(self) -> None:
        timing_info = {f"time_{name}_s": round(value, 3) for name, value in sorted(self.timings.items())}
        logger.info("Runtime metrics summary", **timing_info, **self.counters)


@contextmanager
def timed(metrics: RuntimeMetrics, key: str) -> Any:
    """Measure elapsed wall clock time and store it in runtime metrics."""
    start = time.perf_counter()
    try:
        yield
    finally:
        metrics.add_timing(key, time.perf_counter() - start)


def build_layer_group_lookup(df_wms: pd.DataFrame) -> dict[str, str | None]:
    """Map each layer name to the 2nd hierarchy segment (group)."""
    lookup: dict[str, str | None] = {}
    for _, row in df_wms.iterrows():
        hier_parts = str(row["Hier_Name"]).split("/")
        lookup[str(row["Name"])] = hier_parts[1] if len(hier_parts) > 1 else None
    return lookup


def to_iso_date(value: str) -> str:
    """Normalize known date formats to ISO-8601 date strings."""
    for fmt in ("%d.%m.%Y", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unknown date format: {value}")


def get_wms_capabilities(url_wms: str) -> tuple[ET.Element, dict[str, str]]:
    """Fetch and parse WMS capabilities XML."""
    response = requests.get(url=url_wms, verify=False, timeout=120)
    response.raise_for_status()
    xml_data = response.content
    root = ET.fromstring(xml_data)
    return root, WMS_NAMESPACE


def extract_layers(
    layer_element: ET.Element,
    namespaces: dict[str, str],
    data: list[list[str]],
    name_hierarchy: str | None = None,
    title_hierarchy: str | None = None,
) -> None:
    """Traverse WMS layer hierarchy and store leaf layers."""
    # Find the name and title of the current layer
    name_element = layer_element.find("wms:Name", namespaces)
    title_element = layer_element.find("wms:Title", namespaces)

    layer_name = name_element.text if name_element is not None else None
    layer_title = title_element.text if title_element is not None else None

    # If the layer has a name and a title, set up the hierarchy path
    if layer_name is not None and layer_title is not None:
        # Update the hierarchy path
        current_name_hierarchy = f"{name_hierarchy}/{layer_name}" if name_hierarchy else layer_name
        current_title_hierarchy = f"{title_hierarchy}/{layer_title}" if title_hierarchy else layer_title

        # Check whether there are sub-layers
        sublayers = layer_element.findall("wms:Layer", namespaces)

        if sublayers:
            # If there are sublayers, go through them recursively
            for sublayer in sublayers:
                extract_layers(
                    sublayer,
                    namespaces,
                    data,
                    current_name_hierarchy,
                    current_title_hierarchy,
                )
        else:
            # If there are no sub-layers, add the deepest layer to the data
            data.append(
                [
                    layer_name,
                    layer_title,
                    current_name_hierarchy,
                    current_title_hierarchy,
                ]
            )


def process_wms_data(url_wms: str, metrics: RuntimeMetrics) -> pd.DataFrame:
    """Extract WMS layers into a hierarchy dataframe."""
    with timed(metrics, "wms_capabilities"):
        root, namespaces = get_wms_capabilities(url_wms)
    capability_layer = root.find(".//wms:Capability/wms:Layer", namespaces)

    data: list[list[str]] = []
    if capability_layer is not None:
        extract_layers(capability_layer, namespaces, data)
    return pd.DataFrame(data, columns=["Name", "Layer", "Hier_Name", "Hier_Titel"])


def process_wfs_data(wfs: WebFeatureService) -> pd.DataFrame:
    """Extract WFS layer names from capabilities."""
    feature_list = [{"Name": feature} for feature in wfs.contents]
    df_wfs = pd.DataFrame(feature_list)
    df_wfs["Name"] = df_wfs["Name"].str.replace("ms:", "", regex=False)
    return df_wfs


def create_map_links(geometry: Any, p1: str, p2: str) -> str:
    """Create map-links URL from geometry centroid and map tree params."""
    p1 = urllib.parse.quote(p1)
    p2 = urllib.parse.quote(p2)
    if geometry.geom_type == "Polygon":
        centroid = geometry.centroid
    else:
        centroid = geometry

    lat, lon = centroid.y, centroid.x
    return f"https://opendatabs.github.io/map-links/?lat={lat}&lon={lon}&p1={p1}&p2={p2}"


def get_metadata_cat(df: pd.DataFrame, thema: str) -> Any | None:
    filtered_df = df[df["Thema"] == thema]
    if filtered_df.empty:
        return None

    row = filtered_df.iloc[0]
    return row["Aktualisierung"]


def remove_empty_string_from_list(string_list: list[str]) -> list[str]:
    """Remove empty values from semicolon split lists."""
    return [value for value in string_list if value]


def get_selected_metadata_rows() -> list[dict[str, Any]]:
    """Return imported metadata rows, optionally limited by FGI_DATASET_LIMIT."""
    meta_data = pd.read_excel(Path("data") / "Metadata.xlsx", na_filter=False)
    selected = [row for _, row in meta_data.iterrows() if bool(row["import"])]
    dataset_limit_raw = os.getenv("FGI_DATASET_LIMIT")
    if dataset_limit_raw:
        try:
            selected = selected[: int(dataset_limit_raw)]
        except ValueError:
            logger.warning("Ignoring invalid FGI_DATASET_LIMIT", value=dataset_limit_raw)
    return [{"row": row} for row in selected]


def get_selected_groups() -> set[str]:
    """Return selected group names based on imported metadata rows."""
    return {str(entry["row"]["Gruppe"]) for entry in get_selected_metadata_rows()}


def parse_geocat_metadata(xml_data: bytes) -> tuple[str, str, str, str]:
    """Extract selected metadata fields from geocat XML document."""
    root = ET.fromstring(xml_data)
    position_name = root.find(
        ".//gmd:pointOfContact/che:CHE_CI_ResponsibleParty/gmd:positionName/gco:CharacterString",
        GEOCAT_NAMESPACE,
    )
    first_name = root.find(
        ".//gmd:pointOfContact/che:CHE_CI_ResponsibleParty/che:individualFirstName/gco:CharacterString",
        GEOCAT_NAMESPACE,
    )
    description = root.find(
        ".//gmd:abstract/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString",
        GEOCAT_NAMESPACE,
    )
    date_time_node = root.find(
        ".//che:CHE_MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:DateTime",
        GEOCAT_NAMESPACE,
    )
    date_node = root.find(
        ".//che:CHE_MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date",
        GEOCAT_NAMESPACE,
    )
    date_value = (
        date_node.text if date_node is not None else (date_time_node.text if date_time_node is not None else "")
    )
    return (
        position_name.text if position_name is not None else "",
        first_name.text if first_name is not None else "",
        description.text if description is not None else "",
        date_value,
    )


def create_wfs_getfeature_url(wfs_base_url: str, layer_name: str) -> str:
    """Build a WFS GetFeature URL for a single layer in GeoJSON format."""
    typename = layer_name if ":" in layer_name else f"ms:{layer_name}"
    query = urllib.parse.urlencode(
        {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAME": typename,
            "outputFormat": "application/json; subtype=geojson",
        }
    )
    return f"{wfs_base_url}?{query}"


async def fetch_with_retries(
    client: httpx.AsyncClient,
    url: str,
    metrics: RuntimeMetrics,
    metric_name: str,
    retries: int = 3,
) -> bytes:
    """Fetch bytes from URL with a small retry loop."""
    for attempt in range(1, retries + 1):
        try:
            start = time.perf_counter()
            response = await client.get(url)
            response.raise_for_status()
            metrics.add_timing(metric_name, time.perf_counter() - start)
            metrics.inc(f"{metric_name}_requests")
            return response.content
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            metrics.inc(f"{metric_name}_errors")
            if attempt == retries:
                raise exc
            await asyncio.sleep(attempt * 0.5)
    raise RuntimeError(f"Failed to fetch URL after retries: {url}")


async def fetch_layer_geodata(
    client: httpx.AsyncClient,
    layer_name: str,
    metrics: RuntimeMetrics,
    semaphore: asyncio.Semaphore,
) -> tuple[str, gpd.GeoDataFrame | None, str | None]:
    """Fetch and parse one WFS layer to a GeoDataFrame."""
    async with semaphore:
        try:
            if LOG_WFS_REQUESTS:
                logger.info("Fetching WFS layer", layer_name=layer_name)
            payload = await fetch_with_retries(
                client=client,
                url=create_wfs_getfeature_url(URL_WFS, layer_name),
                metrics=metrics,
                metric_name="wfs_getfeature",
            )
            parse_start = time.perf_counter()
            gdf = await asyncio.to_thread(gpd.read_file, io.BytesIO(payload))
            metrics.add_timing("geopandas_parse", time.perf_counter() - parse_start)
            metrics.inc("wfs_layers_loaded")
            if LOG_WFS_REQUESTS:
                logger.info("Fetched WFS layer", layer_name=layer_name, feature_count=len(gdf.index))
            return layer_name, gdf, None
        except Exception as exc:  # noqa: BLE001
            metrics.inc("wfs_layers_failed")
            return layer_name, None, str(exc)


async def fetch_redirect_params(
    client: httpx.AsyncClient,
    source_link: str,
    metrics: RuntimeMetrics,
) -> tuple[str, str]:
    """Resolve a mapbs short link and extract tree params."""
    normalized_link = source_link.replace("www.geo.bs.ch", "https://geo.bs.ch")
    response = await client.get(normalized_link, follow_redirects=True)
    response.raise_for_status()
    metrics.inc("map_redirect_requests")

    query = urllib.parse.parse_qs(urllib.parse.urlparse(str(response.url)).query)
    tree_groups = query.get("tree_groups", [""])[0]
    tree_group_layers = ""
    for key, values in query.items():
        if key.startswith("tree_group_layers_"):
            tree_group_layers = values[0]
            break
    return tree_groups, tree_group_layers


async def fetch_geocat_metadata(
    client: httpx.AsyncClient,
    geocat_uid: str,
    metrics: RuntimeMetrics,
) -> tuple[str, str, str, str]:
    """Fetch geocat XML and parse metadata fields."""
    url = f"https://www.geocat.ch/geonetwork/srv/api/records/{geocat_uid}/formatters/xml"
    payload = await fetch_with_retries(
        client=client,
        url=url,
        metrics=metrics,
        metric_name="geocat_fetch",
    )
    return parse_geocat_metadata(payload)


async def save_geodata_for_layers(
    df_fgi: pd.DataFrame,
    file_path: str,
    no_file_copy: bool,
    metrics: RuntimeMetrics,
) -> None:
    """Fetch datasets, build geopackages, and prepare ODS metadata."""
    df_cat = pd.read_csv(Path("data") / "100410_geodatenkatalog.csv", sep=";")

    group_to_layers = dict(zip(df_fgi["Gruppe"], df_fgi["Name"], strict=False))
    rows_to_process: list[dict[str, Any]] = []
    for entry in get_selected_metadata_rows():
        row = entry["row"]
        layers = remove_empty_string_from_list(str(row["Layers"]).split(";"))
        if not layers:
            layers = group_to_layers.get(row["Gruppe"], [])
        rows_to_process.append({"row": row, "layers": layers})

    logger.info("Processing datasets", datasets=len(rows_to_process))
    metadata_for_ods: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    geocat_cache: dict[str, tuple[str, str, str, str]] = {}
    redirect_cache: dict[str, tuple[str, str]] = {}

    semaphore = asyncio.Semaphore(WFS_CONCURRENCY)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, verify=False) as client:
        geocat_uids = {
            str(entry["row"]["geocat"]).rsplit("/", 1)[-1].replace("\t", "")
            for entry in rows_to_process
            if str(entry["row"]["geocat"]).strip()
        }
        geocat_tasks = [fetch_geocat_metadata(client, uid, metrics) for uid in geocat_uids]
        geocat_values = await asyncio.gather(*geocat_tasks, return_exceptions=True)
        for uid, value in zip(geocat_uids, geocat_values, strict=False):
            if isinstance(value, Exception):
                logger.warning("Could not fetch geocat metadata", geocat_uid=uid, error=str(value))
                geocat_cache[uid] = ("", "", "", "")
            else:
                geocat_cache[uid] = value

        redirect_links = {
            str(entry["row"]["mapbs_link"])
            for entry in rows_to_process
            if bool(entry["row"]["create_map_urls"]) and str(entry["row"]["mapbs_link"]).strip()
        }
        redirect_tasks = [fetch_redirect_params(client, link, metrics) for link in redirect_links]
        redirect_values = await asyncio.gather(*redirect_tasks, return_exceptions=True)
        for link, value in zip(redirect_links, redirect_values, strict=False):
            if isinstance(value, Exception):
                logger.warning("Could not resolve map redirect", map_link=link, error=str(value))
                redirect_cache[link] = ("", "")
            else:
                redirect_cache[link] = value

        for entry in rows_to_process:
            row = entry["row"]
            layers = entry["layers"]
            dataset_title = str(row["titel_nice"])
            logger.info("Loading dataset layers", dataset=dataset_title, layer_count=len(layers))

            tasks = [fetch_layer_geodata(client, layer, metrics, semaphore) for layer in layers]
            results = await asyncio.gather(*tasks)

            loaded_frames: list[gpd.GeoDataFrame] = []
            for layer_name, gdf, error in results:
                if error:
                    failed.append({"Layer": layer_name, "Error": error})
                    logger.error("Error loading layer", layer=layer_name, error=error)
                    continue
                if gdf is not None:
                    loaded_frames.append(gdf)

            if loaded_frames:
                with timed(metrics, "concat_geodataframes"):
                    merged = pd.concat(loaded_frames, ignore_index=True)
                    gdf_result = gpd.GeoDataFrame(merged, geometry="geometry", crs=loaded_frames[0].crs)
            else:
                gdf_result = gpd.GeoDataFrame()

            if bool(row["create_map_urls"]) and not gdf_result.empty:
                tree_groups, tree_group_layers = redirect_cache.get(str(row["mapbs_link"]), ("", ""))
                if tree_groups and tree_group_layers:
                    gdf_result = gdf_result.to_crs(epsg=4326)
                    gdf_result["Map Links"] = gdf_result.apply(
                        lambda row2: create_map_links(row2["geometry"], tree_groups, tree_group_layers),
                        axis=1,
                    )

            title = str(row["Gruppe"])
            title_dir = Path(file_path) / title
            title_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{row['Dateiname']}.gpkg"
            geopackage_file = title_dir / file_name
            if gdf_result.empty:
                logger.warning("Skipping dataset with no features", dataset=dataset_title)
                continue
            save_gpkg(gdf_result, file_name, str(geopackage_file), metrics)
            if not no_file_copy:
                common.upload_ftp(str(geopackage_file), FTP_SERVER, FTP_USER, FTP_PASS, "harvesters/GVA/data")

            aktualisierung = get_metadata_cat(df_cat, title)
            if pd.isna(aktualisierung) or str(aktualisierung).strip() == "":
                aktualisierung_iso = ""
            else:
                aktualisierung_iso = to_iso_date(str(aktualisierung).strip())

            geocat_uid = str(row["geocat"]).rsplit("/", 1)[-1].replace("\t", "")
            publ_org, herausgeber, geocat_description, dcat_created = geocat_cache.get(geocat_uid, ("", "", "", ""))
            ods_id = row["ods_id"]
            schema_file = f"{ods_id}.csv" if row["schema_file"] else ""
            description = row["beschreibung"]
            dcat_ap_ch_domain = str(row["dcat_ap_ch.domain"]) if str(row["dcat_ap_ch.domain"]) != "" else ""
            tag_values = [tag for tag in str(row["tags"]).split(";") if tag] + ["opendata.swiss"]

            metadata_for_ods.append(
                {
                    "ods_id": ods_id,
                    "name": f"{geocat_uid}:{row['Dateiname']}",
                    "title": row["titel_nice"],
                    "description": description if len(description) > 0 else geocat_description,
                    "theme": str(row["theme"]),
                    "keyword": str(row["keyword"]),
                    "dcat_ap_ch.domain": dcat_ap_ch_domain,
                    "dcat_ap_ch.rights": "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
                    "dcat_ap_ch.license": "terms_by",
                    "dcat.contact_name": "Open Data Basel-Stadt",
                    "dcat.contact_email": "opendata@bs.ch",
                    "dcat.created": dcat_created,
                    "dcat.creator": herausgeber,
                    "dcat.accrualperiodicity": row["dcat.accrualperiodicity"],
                    "attributions": "Geodaten Kanton Basel-Stadt",
                    "publisher": herausgeber,
                    "dcat.issued": row["dcat.issued"],
                    "dcat.relation": "; ".join(filter(None, [row["mapbs_link"], row["geocat"], row["referenz"]])),
                    "modified": aktualisierung_iso,
                    "language": "de",
                    "publizierende-organisation": publ_org,
                    "tags": ";".join(dict.fromkeys(tag_values)),
                    "geodaten-modellbeschreibung": row["modellbeschreibung"],
                    "source_dataset": f"https://data-bs.ch/opendatasoft/harvesters/GVA/data/{file_name}",
                    "schema_file": schema_file,
                }
            )

    pd.DataFrame(failed).to_excel("data/wfs_failed_layers.xlsx", index=False)
    if not metadata_for_ods:
        logger.info("Harvester file contains no entries, no upload necessary.")
        return

    ods_metadata = pd.DataFrame(metadata_for_ods)
    ods_metadata_filename = Path("data") / "Opendatasoft_Export_GVA_GPKG.csv"
    ods_metadata.to_csv(ods_metadata_filename, index=False, sep=";", encoding="utf-8")

    if ct.has_changed(str(ods_metadata_filename)) and (not no_file_copy):
        logger.info("Uploading ODS harvester file", file=str(ods_metadata_filename))
        common.upload_ftp(str(ods_metadata_filename), FTP_SERVER, FTP_USER, FTP_PASS, "harvesters/GVA")
        ct.update_hash_file(str(ods_metadata_filename))

    logger.info("Uploading ODS schema files to FTP server...")
    for schemafile in ods_metadata["schema_file"].unique():
        if schemafile == "":
            continue
        schemafile_with_path = Path("data") / "schema_files" / schemafile
        if ct.has_changed(str(schemafile_with_path)) and (not no_file_copy):
            logger.info("Uploading ODS schema file", file=str(schemafile_with_path))
            common.upload_ftp(str(schemafile_with_path), FTP_SERVER, FTP_USER, FTP_PASS, "harvesters/GVA")
            ct.update_hash_file(str(schemafile_with_path))


async def get_name_col(df_wfs: pd.DataFrame, metrics: RuntimeMetrics) -> None:
    """Write schema templates containing all columns per layer."""
    semaphore = asyncio.Semaphore(WFS_CONCURRENCY)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, verify=False) as client:
        tasks = [fetch_layer_geodata(client, str(row["Name"]), metrics, semaphore) for _, row in df_wfs.iterrows()]
        results = await asyncio.gather(*tasks)

    layer_to_group = dict(zip(df_wfs["Name"], df_wfs["Gruppe"], strict=False))
    for layer, gdf, error in results:
        group = layer_to_group.get(layer)
        if error or gdf is None or group is None:
            logger.error("Schema extraction failed", layer=layer, error=error)
            continue
        folder_path = Path("data") / "schema_files" / "templates" / str(group)
        folder_path.mkdir(parents=True, exist_ok=True)
        columns = gdf.columns.tolist()
        schema_df = pd.DataFrame(
            {"name": columns, "label": ["" for _ in columns], "description": ["" for _ in columns]}
        )
        schema_df.to_csv(folder_path / f"{layer}.csv", index=False, sep=";")
    logger.info("All schema templates were saved successfully.")


async def get_num_col(df_fgi: pd.DataFrame, metrics: RuntimeMetrics) -> None:
    """Write csv with number of columns per layer and group."""
    semaphore = asyncio.Semaphore(WFS_CONCURRENCY)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, verify=False) as client:
        for _, row in df_fgi.iterrows():
            tasks = [fetch_layer_geodata(client, layer, metrics, semaphore) for layer in row["Name"]]
            outputs = await asyncio.gather(*tasks)
            results: list[dict[str, Any]] = []
            for layer_name, gdf, error in outputs:
                if error or gdf is None:
                    results.append({"Layer": layer_name, "Anzahl_Spalten": "Fehler"})
                else:
                    results.append({"Layer": layer_name, "Anzahl_Spalten": gdf.shape[1]})
            file_path = Path("data") / "schema_files" / "templates" / str(row["Gruppe"]) / "Anzahl der Spalten.csv"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(results).to_csv(file_path, index=False, sep=";")


def save_gpkg(gdf: gpd.GeoDataFrame, file_name: str, final_gpkg_path: str, metrics: RuntimeMetrics) -> None:
    """Persist a GeoDataFrame as GPKG via temp path then copy."""
    temp_dir = Path("temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_gpkg_path = temp_dir / file_name
    with timed(metrics, "gpkg_write"):
        gdf.to_file(temp_gpkg_path, driver="GPKG")
    shutil.copy(temp_gpkg_path, final_gpkg_path)
    if temp_gpkg_path.exists():
        temp_gpkg_path.unlink()


def ods_id_col(df_wfs: pd.DataFrame, df_fgi: pd.DataFrame) -> pd.DataFrame:
    """Build and merge ods_id mapping for each WFS layer."""
    meta_data = pd.read_excel(Path("data") / "Metadata.xlsx", na_filter=False)
    group_to_layers = dict(zip(df_fgi["Gruppe"], df_fgi["Name"], strict=False))
    available_layers = set(df_wfs["Name"].to_list())
    layer_data: list[dict[str, Any]] = []
    for _, row in meta_data.iterrows():
        shapes_to_load = remove_empty_string_from_list(str(row["Layers"]).split(";"))
        if not shapes_to_load:
            shapes_to_load = group_to_layers.get(row["Gruppe"], [])
        for layer in shapes_to_load:
            if layer in available_layers:
                layer_data.append({"Name": layer, "ods_ids": row["ods_id"]})

    if not layer_data:
        df_wfs["ods_ids"] = None
        return df_wfs
    layer_mapping = pd.DataFrame(layer_data)
    grouped_mapping = layer_mapping.groupby("Name", as_index=False)["ods_ids"].apply(list)
    return df_wfs.merge(grouped_mapping, on="Name", how="left")


async def async_main(no_file_copy: bool) -> None:
    """Run the full ETL workflow."""
    metrics = RuntimeMetrics()
    selected_groups = get_selected_groups()
    generate_schemas = os.getenv("FGI_GENERATE_SCHEMAS", "true").lower() in {"1", "true", "yes"}

    with timed(metrics, "wfs_capabilities"):
        wfs = WebFeatureService(url=URL_WFS, version="2.0.0", timeout=120)
    df_wms = process_wms_data(URL_WMS, metrics)
    df_wfs = process_wfs_data(wfs)

    layer_group_lookup = build_layer_group_lookup(df_wms)
    df_wfs["Gruppe"] = df_wfs["Name"].map(layer_group_lookup)
    df_wfs = df_wfs[["Gruppe", "Name"]]
    df_wms_not_in_wfs = df_wms[~df_wms["Name"].isin(df_wfs["Name"])]
    df_fgi = df_wfs.groupby("Gruppe", dropna=False)["Name"].apply(list).reset_index()
    df_wfs = ods_id_col(df_wfs, df_fgi)

    df_wms.to_csv(Path("data") / "Hier_wms.csv", sep=";", index=False)
    df_fgi.to_csv(Path("data") / "mapBS_shapes.csv", sep=";", index=False)
    df_wms_not_in_wfs.to_csv(Path("data") / "wms_not_in_wfs.csv", sep=";", index=False)
    path_export = Path("data") / "100395_OGD_datensaetze.csv"
    df_wfs.to_csv(path_export, sep=";", index=False)
    if not no_file_copy:
        common.update_ftp_and_odsp(str(path_export), "opendatabs", "100395")

    if generate_schemas:
        df_wfs_selected = df_wfs[df_wfs["Gruppe"].isin(selected_groups)].copy()
        df_fgi_selected = df_fgi[df_fgi["Gruppe"].isin(selected_groups)].copy()
        with timed(metrics, "schema_name_generation"):
            await get_name_col(df_wfs_selected, metrics)
        with timed(metrics, "schema_count_generation"):
            await get_num_col(df_fgi_selected, metrics)
    else:
        logger.info("Skipping schema generation (FGI_GENERATE_SCHEMAS=false)")

    await save_geodata_for_layers(df_fgi, str(Path("data") / "export"), no_file_copy, metrics)
    metrics.log_summary()


def main() -> None:
    """Entrypoint for synchronous script execution."""
    no_file_copy = "no_file_copy" in sys.argv
    logger.info("Executing job", no_file_copy=no_file_copy, script=__file__)
    asyncio.run(async_main(no_file_copy=no_file_copy))
    logger.info("Job successful")


if __name__ == "__main__":
    os.environ.setdefault("IS_PROD", "false")
    import logging

    logging.getLogger("httpx").setLevel(logging.INFO if LOG_WFS_REQUESTS else logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    init_logger()
    logger.info("WFS logging mode", fgi_log_wfs=LOG_WFS_REQUESTS)
    main()
