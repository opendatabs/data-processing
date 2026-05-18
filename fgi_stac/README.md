# fgi_stac

Pipeline: STAC/Dataspot → Katalog/Schema/GeoJSON → HUWISE (FTP).

## Ablauf

```mermaid
flowchart TB
  subgraph job1 [Job 1: sync_catalog]
    STAC["STAC API\napi.geo.bs.ch"]
    DSmeta["Dataspot Metadaten"]
    STAC --> excel["data/huwise_bindings.xlsx"]
    DSmeta --> catalog["data_orig/publish_catalog.yaml"]
  end

  subgraph job2 [Job 2: prepare_assets]
    STACdl["STAC GeoJSON Download"]
    TR["transforms/*.py"]
    SCH["Schema YAML\norig + data/schema_files"]
    MAP["MapBS map_links"]
    STACdl --> TR --> geo["data/datasets/*.geojson"]
    TR --> SCH
    TR --> MAP
  end

  subgraph job3 [Job 3: publish]
    FTP["FTP fgi/stac"]
    HW["HUWISE API\n+ publish()"]
    geo --> FTP
    geo --> HW
    catalog --> HW
    SCH --> HW
  end

  excel --> job2
  catalog --> job2
  job1 --> job2
  job2 --> job3
```



## Befehle


| Zweck                             | Befehl                                                                                 |
| --------------------------------- | -------------------------------------------------------------------------------------- |
| Nur Katalog + Excel               | `uv run sync_catalog.py`                                                               |
| GeoJSON + Schema + Map-Links      | `uv run prepare_assets.py`                                                             |
| FTP + HUWISE                      | `uv run publish.py`                                                                    |
| Ein Datensatz (prepare + publish) | `uv run prepare_assets.py --huwise-id 100095` / `uv run publish.py --huwise-id 100095` |
| Vollpipeline (Airflow)            | `uv run etl.py`                                                                        |
| Vollpipeline, ein Datensatz       | `uv run etl.py --huwise-id 100095`                                                     |


Reihenfolge in Airflow: `sync_catalog` → `prepare_assets` → `publish`. Job 2 und 3 können mit `--huwise-id` pro Datensatz laufen.

## Exchange-Ordner

**Exchange-Ordner/StatA/FGI/STAC**


| Bearbeiten                                                      | Nicht bearbeiten                         |
| --------------------------------------------------------------- | ---------------------------------------- |
| `data/huwise_bindings.xlsx` → Spalte `**huwise_id`**            | `data_orig/**` (Pipeline schreibt neu)   |
| `data/schema_files/*.yaml` → Feld-Schema, siehe unten           | Datensatz-Metadaten nicht in Excel/YAML  |
| **HUWISE-Datenportal** → nur Datensatz-Metadaten (nicht Schema) | Datensatzschema nur via YAML + `publish` |


### Datensatz-Metadaten (HUWISE-Portal)

Metadaten des Datensatzes (Titel, Beschreibung, Schlagwörter, Lizenz, Kontakt, Tags, DCAT-Felder usw. — **nicht** das Spalten-Schema) werden **im HUWISE-Datenportal** am Datensatz gepflegt.

Beim Publish setzt die Pipeline diese Metadaten **konservativ**: Portal-Änderungen bleiben erhalten, solange sie vom letzten automatischen Lauf abweichen. Referenz dafür ist `data_orig/publish_metadata_last_push.yaml` (nur lesen, von der Pipeline geschrieben). STAC/Dataspot füllen weiterhin leere Felder aus dem Katalog nach.

### Feld-Schema (`data/schema_files/*.yaml`)

Das **Datensatzschema** (Feldtypen, Spaltennamen, Beschriftungen, Prozessoren im Portal) kommt aus `data/schema_files/*.yaml` und wird beim Job `**publish`** in HUWISE gesetzt.

**Änderungen am Datensatzschema direkt im HUWISE-Datenportal werden beim nächsten Lauf überschrieben**. Schema-Anpassungen daher nur in den YAML-Dateien im Exchange-Ordner vornehmen, nicht im Portal.

Pro Eintrag unter `fields:` (Editorial-Overrides; Dataspot-Spaltenname steht in `dataspot_attribute`):


| Feld             | Typ                            | Bedeutung                                                                                                                                  |
| ---------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `export`         | **boolean** (`true` / `false`) | Spalte in HUWISE publizieren (`true`) oder nur im Schema führen (`false`). Steuert auch `map_links` (Karten-Links nur bei `export: true`). |
| `technical_name` | string                         | Technischer Spaltenname in HUWISE / GeoJSON nach Rename                                                                                    |
| `name`           | string                         | Anzeigename des Felds im Portal                                                                                                            |
| `description`    | string                         | Feldbeschreibung im Portal                                                                                                                 |
| `mehrwertigkeit` | string                         | Trennzeichen bei Mehrfachwerten (z. B. `;`), nur relevant für Textfelder                                                                   |
| `datentyp`       | string                         | HUWISE-Feldtyp (`text`, `int`, `double`, `geo_shape`, `geo_point_2d`, `date`, `datetime` …) — muss zur Geometrie/Spalte passen             |


Nicht in den Schema-YAMLs editieren: `huwise_id`, `dataspot_asset_url`, `stac_url` (werden von der Pipeline gepflegt).

Optional: `transforms/*.py` (sonst reicht `transforms/_default.py`).

## Ordner


| Pfad                                             | Rolle                                                             |
| ------------------------------------------------ | ----------------------------------------------------------------- |
| `data_orig/publish_catalog.yaml`                 | Maschinen-Katalog (nur aktive `huwise_id`)                        |
| `data_orig/publish_metadata_last_push.yaml`      | Letzter erfolgreicher HUWISE-Metadaten-Stand                      |
| `data_orig/datasets/`, `data_orig/schema_files/` | STAC-Rohdaten, Dataspot-Schema                                    |
| `data/huwise_bindings.xlsx`                      | STAC-Index + `**huwise_id`**                                      |
| `data/schema_files/`                             | Publish-Schema (Editorial: `export`, `technical_name`, `name`, …) |
| `data/datasets/`                                 | GeoJSON nach Transform (Publish-Input)                            |


## Fehler nachvollziehen

Logs nutzen `STEP …`-Zeilen pro Phase:


| Job              | Typische Log-Zeilen                                        | Artefakte prüfen                                              |
| ---------------- | ---------------------------------------------------------- | ------------------------------------------------------------- |
| `sync_catalog`   | `STEP sync_catalog start/done`                             | `data/huwise_bindings.xlsx`, `data_orig/publish_catalog.yaml` |
| `prepare_assets` | `STEP prepare_assets huwise_id=…`                          | `data/datasets/*.geojson`, `data/schema_files/*.yaml`         |
| `publish`        | `STEP publish_dataset`, `upload_geojson`, `publish_schema`, `publish_huwise` | GeoJSON auf FTP, Schema-YAML; HUWISE-Portal nach `HuwiseDataset.publish()` |


- Job 2 ohne Job 1: Fehler „Missing publish catalog“ → zuerst `uv run sync_catalog.py`.
- HUWISE „Internal error while processing“: `export` und `datentyp` passend zur Geometrie; gültiges YAML in `data/schema_files`.

