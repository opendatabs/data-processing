# Harvester for GVA Gedodata files

## Publish existing GVA datasets

### Description of the files

- `Metadata.xlsx`: This file contains the remaining metadata for one or more datasets.
- `Ogd_datensaetze.csv` : This file lists the shapes available for publication as OGD, including groups (e.g., "Velo") and the layers within these groups (e.g., "VO_Velostrasse_Abfrage").Include information on which shapes have already been published and to which dataset they belong for better tracking.
- `mapBS_shapes.csv` : This file maps datasets to their corresponding groups as in MapBS, helping to identify which layers belong to which groups.

### Identify dataset to publish in `Ogd_datensaetze.csv`

- GVA retrieves all available geo datasets every 30 minutes via WFS from Mapbs ([https://map.geo.bs.ch/](https://map.geo.bs.ch/)). More details can be found at [https://www.bs.ch/bvd/grundbuch-und-vermessungsamt/geo/geodaten/geodienste#wfsbs](https://www.bs.ch/bvd/grundbuch-und-vermessungsamt/geo/geodaten/geodienste#wfsbs).

- Open file `Ogd_datensaetze.csv` located in `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\FGI` in Excel , find the dataset to be published as OGD, and select which shapes (Name) you want to publish. Copy the group(Gruppe) information associated with these shapes.

- Before copying the contents, review `mapBS.csv`, which is located in the same directory, to check how many shapes each group contains.

- It is possible that shapes within a group have different columns. To determine if they can be merged into a single shape or to check how many columns each shape has, refer to the folder `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\FGI\schema_files\tamplates`.

### Fill out file `Metadata.xlsx`

- Open file `Metadata.xlsx` located in `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\FGI` in Excel.
- Add a new row, paste contents of column "Gruppe" copied from the selected row in File `ogd_datensaetze.csv`.
- Set "import" to "True".
- Column "Layers": Define which shape via wfs should be imported. Leave empty to import all shape to explore the shapes in ODS before publication.  Do not add file extension. Multiple shapes can be separated with semicolon. Do not add a semicolon at the end of a list of shape names. &#x20;
- The "Dateiname" column is intended to specify the storage name for the shape.
- Column "title\_nice": Replace shape names as title of ODS datasets.
- Column "ods\_id": Dataset id that will be used in ODS. Currently, this id is not automatically set and is just used for reference.
- Column "beschreibung": Add a description text for the shape(s) in question. If no description is given, the description by GVA is used.
- Column "referenz": Add URL that will be set as "Reference" in ODS. If left empty, this should be filled out automatically as "[https://geo.bs.ch/](https://geo.bs.ch/)...".
- Column "theme": ODS / opendata.swiss theme(s) in German.
- Column "keyword": Semicolon-separated list of keywords to be used in ODS.
- Column "dcat\_ap\_ch.domain": Used if the dataset should be assigned to an opendata.swiss suborganisation.
- Column "dcat.accrualperiodicity": Accrual periodicity as described [here](https://handbook.opendata.swiss/de/content/glossar/bibliothek/dcat-ap-ch.html?highlight=accrual)
- Column "schema_file": Set "True" if a (schema file)[[https://help.opendatasoft.com/platform/en/publishing_data/02_harvesting_a_catalog/harvesters/ftp_with_meta_csv.html#schema-csv-file]](https://userguide.opendatasoft.com/l/en/article/wsyubsjp1m-ftp-with-meta-csv-harvester#schema_csv_file) is provided in the [schema_files](data/schema_files/) folder. Schema files must be named `{ods\_id}.csv`. However, there are ready-made templates available for each shape, stored in`{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\FGI\schema_files.`
- Column "create\_map\_urls": Set "True" if links to various map services for the specified coordinates should be provided. Example: [https://opendatabs.github.io/map-links/?lat=47.564901&lon=7.615269](https://opendatabs.github.io/map-links/?lat=47.564901\&lon=7.615269)
- Column "dcat.issued": Date string in the form "JJJJ-MM-TT" to be used as issued date in ODS and opendata.swiss.
- Column "tags": If left empty, just opendata.swiss will be filled as tag. Recommended if several datasets with same topic are published.

### Deployment and harvesting

- If schema\_file is added or changed, the Airflow Job 'stata\_pull\_changes' also has to be run.
- Start Airflow Job `fgi-geodatenshop`. Shapes are uploaded to FTP, and ODS harvester is started.
- After successful finish of ODS harvester: In Backoffice, check newly created dataset(s), change metadata in file `Metadata.xlsx` accordingly.
- Manually change ODS id of newly datasets. To do this, you have to depublish the dataset first.
- Newly created datasets are not auto-published, but remain private until published in ODS.
- Changes in datasets that have been published in ODS before are automatically published when the ODS harvester has finished running.
- Repeat until happy with the results ;-)

### Inform Key People at[ ](https://geo.bs.ch)[https://geo.bs.ch](https://geo.bs.ch) and Data Owners

- Contact the relevant key person a[t ](https://geo.bs.ch)[https://geo.bs.ch](https://geo.bs.ch) (via phone/Teams or in person) about our intent to publish some of the data harvested from their portal and the planned publication date o[n ](https://data.bs.ch)[https://data.bs.ch](https://data.bs.ch).
- Discuss whether they will inform the respective data owners about the upcoming publication of their data on[ ](https://data.bs.ch)[https://data.bs.ch](https://data.bs.ch), or whether we should do this ourselves. By default, we do this ourselves via (phone/Teams or in person).

### Publishing Dataset

- Follow standard procedures to prepare dataset(s) for publication in ODS

