# eLeistungen-Übersicht

## Data Owner

Digilab

## Beschreibung

Dieses Skript erstellt eine **Excel-Übersicht**, die zeigt, welche Organisationseinheiten des Kantons Basel-Stadt bereits elektronische Leistungen (eLeistungen) anbieten und welche nicht.

Dazu werden zwei Datensätze vom Open-Data-Portal [data.bs.ch](https://data.bs.ch) zusammengeführt:

| Datensatz | ODS-ID | Inhalt |
|---|---|---|
| [Staatskalender](https://data.bs.ch/explore/dataset/100349/) | 100349 | Offizielle Organisationsstruktur des Kantons (Departemente, Dienststellen, Abteilungen usw.) |
| [eLeistungen-Katalog](https://data.bs.ch/explore/dataset/100324/) | 100324 | Verzeichnis aller elektronischen Verwaltungsleistungen mit Zuordnung zu Dienststellen |

Da die Namen der Dienststellen in den beiden Datensätzen nicht immer exakt übereinstimmen, nutzt das Skript eine automatische **Namensähnlichkeits-Suche** (Fuzzy Matching), um die Leistungen den richtigen Organisationseinheiten zuzuordnen.

Anschliessend wird anhand des offiziellen Organigramms (Stand 1. Januar 2026) gefiltert, damit nur aktuelle Dienststellen in der Auswertung erscheinen.

## Quelldaten

Beide Datensätze werden über die ODS-API heruntergeladen. Der Zugang erfordert einen **API-Key**, der als Umgebungsvariable `ODS_API_KEY` in der Datei `.env` hinterlegt sein muss:

```
ODS_API_KEY=dein_api_key_hier
```

## Ergebnis

Das Skript erzeugt die Datei **`data/leistungen_uebersicht.xlsx`** mit vier Tabellenblättern:

### 1. Alle Organisationen

Die vollständige Organisationshierarchie mit farblicher Kennzeichnung:

| Farbe | Bedeutung |
|---|---|
| Grün | Die Organisation bietet selbst eLeistungen an |
| Gelb | Nur untergeordnete Einheiten bieten eLeistungen an |
| Rot | Weder die Organisation noch ihre Untereinheiten bieten eLeistungen an |

Spalten: Ebene 1–4, Weitere Ebenen, ID, Name, Tiefe, Anzahl direkte Leistungen, Anzahl inkl. Unterorganisationen, Leistungsnamen.

### 2. Departemente

Zusammenfassung pro Departement: Anzahl Dienststellen mit/ohne eLeistungen, Leistungen des Generalsekretariats und der Departementsleitung sowie Gesamtzahl.

### 3. Dienststellen

Zusammenfassung pro Dienststelle innerhalb jedes Departements mit der jeweiligen Anzahl eLeistungen.

### 4. Leistungen Detail

Der vollständige eLeistungen-Katalog (Rohdaten) inklusive der Zuordnung zum Staatskalender.
