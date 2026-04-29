# fgi_stac

YAML-first Pipeline zum Aktualisieren des Katalogs (STAC, Dataspot, Geometa) und zum Publizieren nach HUWISE inkl. FTP-Upload der GeoJSON-Dateien.

## Wo die Daten liegen (Exchange)

Die **fachlichen Quelldaten** (GeoJSON-Dateien, die veröffentlicht werden) werden **nicht** „irgendwo im Git“ bearbeitet, sondern im Austauschordner:

**`Exchange-Ordner/StatA/FGI/STAC`**

Dort die Dateien pflegen, die später unter `https://data-bs.ch/stata/fgi/stac/…` bereitstehen. Im Repository liegen die GeoJSON-Abzüge unter `data/datasets/` für die Pipeline (Schema-Abgleich, lokale Läufe, CI); Änderungen für den Betrieb sollten konsistent mit dem Exchange-Stand erfolgen (z. B. nach Bearbeitung im Exchange ins Repo kopieren oder umgekehrt, je nach internem Ablauf).

---

## Neue Datensätze hinzufügen

### Ablauf (kurz)

1. **Voraussetzung:** Der Datensatz existiert in **Dataspot** und erscheint in der **STAC-Collection** auf `api.geo.bs.ch` (inkl. Eintrag in der Geometa-HTML-Vorschau der Collection).
2. **Katalog erzeugen/aktualisieren:** `uv run etl.py` (oder bei Bedarf nur Metadaten-Refresh mit `--refresh-only`). Das Skript liest STAC/Dataspot und schreibt `data/publish_catalog.yaml`.
3. **HUWISE anbinden:** Für jeden Datensatz, der publiziert werden soll, im YAML unter dem jeweiligen `geo_datasets`-Eintrag **`huwise_id`** setzen (ODS-ID). Ohne `huwise_id` wird der Datensatz beim Publish übersprungen.
4. **Publish:** `uv run etl.py` (ohne `--refresh-only`) oder direkt `uv run python publish_dataset.py`.

---

## Schema anpassen

Schemas für HUWISE-Pflichtdatensätze liegen unter `data/schema_files/*.yaml`. Pro Feld sind u. a. vorgesehen:

- `technical_name`, `name`, `description`, `mehrwertigkeit`, `datentyp`
- `custom` mit `technical_name`, `name`, `description`
- `export` (Standard `true`, für `gdh_fid` typischerweise `false`)

Beim Lauf von **`etl.py`** werden Schema-Dateien für Einträge **mit** `huwise_id` neu aus Dataspot + lokalem GeoJSON zusammengeführt und die YAML-Datei überschrieben. Eigene Anpassungen bleiben erhalten, soweit sie im bestehenden Katalog/Schema schon als „old“-Stand eingebunden sind (Zusammenführung mit Dataspot).

**Felder nur unter custom ändern.**

---

## Befehle

Katalog aktualisieren **und** Publish ausführen:

```bash
uv run etl.py
```

Nur Katalog neu aus STAC/Dataspot schreiben (kein HUWISE/FTP):

```bash
uv run python etl.py --refresh-only
```

Publish trocken testen:

```bash
uv run python etl.py --dry-run
```

Direkt nur Publish (Katalog muss bereits passen):

```bash
uv run python publish_dataset.py
```

Metadaten in HUWISE für alle konfigurierten Felder erzwingend überschreiben (nur bei Bedarf):

```bash
uv run python publish_dataset.py --force-metadata-sync
```

---

## HUWISE-Metadaten und `publish_metadata_last_push.yaml`

`publish_dataset.py` setzt Metadatenfelder nur, wenn HUWISE leer ist, der Wert schon dem gewünschten Stand entspricht (Katalog + Dataspot), oder der aktuelle HUWISE-Wert noch dem zuletzt von diesem Skript erfolgreich geschriebenen Wert entspricht (Snapshot in `data/publish_metadata_last_push.yaml`). So können Katalog-/Dataspot-Änderungen nachgezogen werden, ohne manuelle Portal-Anpassungen zu überschreiben, sobald sich HUWISE vom letzten Push unterscheidet.

Nach jedem Lauf ohne `--dry-run` wird die Snapshot-Datei aktualisiert; sie sollte ins Repository committet werden, damit CI und lokale Läufe dasselbe Verhalten zeigen.
