# fgi_stac

YAML-first Pipeline zum Aktualisieren des Katalogs (STAC, Dataspot, Geometa) und zum Publizieren nach HUWISE inkl. FTP-Upload der GeoJSON-Dateien.

## Wo die Daten liegen (Exchange)

Die **fachlichen Quelldaten** (GeoJSON-Dateien, die veröffentlicht werden und die YAML mit allen Metadaten) werden **nicht** "irgendwo im Git" bearbeitet, sondern im Austauschordner:

**`Exchange-Ordner/StatA/FGI/STAC`**

---

## Neue Datensätze hinzufügen

### Ablauf (kurz)

1. **Voraussetzung:** Der Datensatz existiert in **Dataspot** und erscheint in der **STAC-Collection** auf `api.geo.bs.ch` (inkl. Eintrag in der Geometa-HTML-Vorschau der Collection).
2. **Katalog erzeugen/aktualisieren:** `uv run etl.py` (oder bei Bedarf nur Metadaten-Refresh mit `--refresh-only`). Das Skript liest STAC/Dataspot und schreibt `data/publish_catalog.yaml`.
3. **HUWISE anbinden:** Im YAML unter `geo_datasets` beim gewünschten Eintrag `huwise_id` setzen (ODS-ID). Ohne `huwise_id` wird der Datensatz beim Publish übersprungen.
4. **Erster Lauf:** `uv run etl.py` (i.e. `fgi_stac` in Airflow) erzeugt/aktualisiert Schema-Dateien und publiziert.
5. **Schema-Feinschliff:** Nur `custom`-Werte in `data/schema_files/*.yaml` bearbeiten.
6. **Zweiter Lauf:** `uv run etl.py` (i.e. `fgi_stac` in Airflow) erneut ausführen, damit `custom`-Overrides in HUWISE angewendet werden.
7. **Metadata:** Manuelle redaktionelle Anpassungen erfolgen in HUWISE.

---

## Was darf editiert werden?

### `data/publish_catalog.yaml`

Erlaubt ist primär das Setzen/Aendern von `huwise_id`.

```yaml
datasets:
  - stac_collection_id: AFBA
    geo_datasets:
      - dataspot_dataset_id: a396da69-b42e-463b-8dab-95812235c607
        geo_dataset: Abfuhrzonen
        huwise_id: 100095stac  # OK: darf gesetzt/geaendert werden
        metadata:              # NICHT manuell pflegen (pipeline-managed)
          default:
            title: ...
```

Do:

- `huwise_id` setzen oder korrigieren.

Don't:

- keine manuellen Struktur-Aenderungen an `metadata`, `dataspot_*`, `stac_*`.
- keine manuellen Feldlisten/Schema-Inhalte hier pflegen.

### `data/schema_files/*.yaml`

Erlaubt sind inhaltliche Anpassungen unter `fields[].custom`.

```yaml
huwise_id: 100095stac
dataspot_dataset_id: a396da69-b42e-463b-8dab-95812235c607
fields:
  - technical_name: strasse
    name: Strasse
    description: Name der Strasse
    datentyp: text
    custom:
      technical_name: strasse_name   # OK: custom override
      name: Strassenname             # OK: custom override
      description: Amtliche Bezeichnung
```

Do:

- nur `fields[].custom.technical_name`, `fields[].custom.name`, `fields[].custom.description` editieren.

Don't:

- `technical_name`, `name`, `description`, `datentyp`, `mehrwertigkeit`, `export` ausserhalb `custom` nicht manuell ummodellieren.
- keine HUWISE-Schema-Aenderungen direkt im Portal als Source-of-Truth betrachten; Source-of-Truth bleibt diese Datei.

---

## Schema anpassen

Schemas für HUWISE-Pflichtdatensätze liegen unter `data/schema_files/*.yaml`. Pro Feld sind u. a. vorgesehen:

- `technical_name`, `name`, `description`, `mehrwertigkeit`, `datentyp`
- `custom` mit `technical_name`, `name`, `description`
- `export` (Standard `true`, für `gdh_fid` typischerweise `false`)

Beim Lauf von **`etl.py`** werden Schema-Dateien für Einträge **mit** `huwise_id` neu aus Dataspot + lokalem GeoJSON zusammengeführt und die YAML-Datei überschrieben.

**Felder nur unter `custom` ändern.**

---

## Befehle

Katalog aktualisieren **und** Publish ausführen:

```bash
uv run etl.py
```

Nur Katalog neu aus STAC/Dataspot schreiben (kein HUWISE/FTP):

```bash
uv run etl.py --refresh-only
```

Publish trocken testen:

```bash
uv run etl.py --dry-run
```

Direkt nur Publish (Katalog muss bereits passen):

```bash
uv run publish_dataset.py
```

Metadaten in HUWISE für alle konfigurierten Felder erzwingend überschreiben (nur bei Bedarf):

```bash
uv run publish_dataset.py --force-metadata-sync
```

---

## HUWISE-Metadaten und `publish_metadata_last_push.yaml`

`publish_dataset.py` setzt Metadatenfelder nur, wenn HUWISE leer ist, der Wert schon dem gewünschten Stand entspricht (Katalog + Dataspot), oder der aktuelle HUWISE-Wert noch dem zuletzt von diesem Skript erfolgreich geschriebenen Wert entspricht (Snapshot in `data/publish_metadata_last_push.yaml`). So können Katalog-/Dataspot-Änderungen nachgezogen werden, ohne manuelle Portal-Anpassungen zu überschreiben, sobald sich HUWISE vom letzten Push unterscheidet.

Nach jedem Lauf ohne `--dry-run` wird die Snapshot-Datei aktualisiert; sie sollte ins Repository committet werden, damit CI und lokale Läufe dasselbe Verhalten zeigen.

## Ressourcen-Policy

Bestehende HUWISE-Datensätze behalten ihre bestehende Resource-URL. Die Pipeline setzt/aktualisiert die Resource nur bei neu angelegten Datensätzen.
