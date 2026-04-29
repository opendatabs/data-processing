# fgi_stac

Dieses Projekt verarbeitet STAC-Collections und Dataspot-Datensätze, erstellt lokale Artefakte und publiziert nach Huwise inklusive FTP-Upload der GeoJSON-Dateien.

## Zweck

- Metadaten und STAC-Zuordnungen für Huwise pflegen.
- GeoJSON-Ressourcen aufbereiten und publizieren.
- Schema-Definitionen pro Datensatz verwalten.

## Dateien und Verantwortlichkeiten

- `extract_collections.py`: lädt STAC-Collections und schreibt Rohdaten nach `data_orig`.
- `extract_metadata.py`: lädt Dataspot-Metadaten und schreibt Rohdaten nach `data_orig`.
- `etl.py`: erzeugt aus Rohdaten lokale Artefakte (z. B. GeoJSON, `Metadata.csv`).
- `migrate_publish_catalog.py`: baut `publish_catalog.json` und `stac_index.json` (mit Fallback auf bestehenden Katalog).
- `publish_dataset.py`: publiziert Ressourcen, Metadaten und Feldkonfigurationen nach Huwise.
- `data/catalog_editor.py`: Streamlit-Editor für `publish_catalog.json` und Schema-Dateien.
- `paths.py`: zentrale Pfaddefinitionen und Trennung Input/Output.

## Datenablage

- `data_orig`: externe Rohdaten (Input).
- `data`: lokal erzeugte Artefakte (Output).
- `pub_datasets.xlsx` ist optionaler Legacy-Input und wird nur verwendet, wenn vorhanden.

## Lokale Nutzung

Abhängigkeiten installieren:

```bash
uv sync
```

Pflicht-Umgebungsvariablen:

- `HUWISE_API_KEY`
- `FTP_USER_01`
- `FTP_PASS_01`

Optional:

- `HUWISE_DOMAIN` (Default: `data.bs.ch`)
- `HUWISE_API_TYPE` (Default: `automation/v1.0`)

## Wichtige Befehle

Kompletter lokaler Lauf:

```bash
uv run extract_collections.py
uv run extract_metadata.py
uv run etl.py
uv run migrate_publish_catalog.py
uv run publish_dataset.py --dry-run
uv run publish_dataset.py
```

Nur Publish testen (ohne Schreibzugriffe):

```bash
uv run publish_dataset.py --dry-run
```

## Metadaten pflegen (Streamlit)

Editor starten:

```bash
uv run streamlit run data/catalog_editor.py
```

Wichtig:

- Streamlit schreibt in `publish_catalog.json` und die Schema-Dateien.
- Beim Publish werden lokale Werte mit `override_remote_value=true` gesetzt (lokale Werte sind führend).

## Beispiel: Neuen Datensatz über STAC aufnehmen

1. Streamlit öffnen: `uv run streamlit run data/catalog_editor.py`
2. STAC-Collection auswählen.
3. Geodatensatz auswählen.
4. Auf **„Neuen STAC-Datensatz hinzufügen“** klicken.
5. Huwise-ID (im JSON-Feld `ods_id`), Titel, Beschreibung, Keywords und optionale Custom-Felder ergänzen.
6. Speichern.
7. Validieren: `uv run publish_dataset.py --dry-run`
8. Publizieren: `uv run publish_dataset.py`

## Docker

Container lokal bauen:

```bash
docker build -t fgi-stac:latest .
```

Standard-Startkommando im Container:

```bash
uv run python publish_dataset.py
```
