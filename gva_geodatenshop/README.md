# Harvester for GVA Gedodata files

## Publish existing GVA datasets

### Description of the files
- `ogd_datensaetze.csv`: This file lists the datasets that are available for publication as OGD.
- `Publizierende_organisation.csv`: This file contains the responsible "Dienststelle" (department or organization).
- `Metadata.xlsx`: This file contains the remaining metadata for one or more datasets.

### Identify dataset to publish in `ogd_datensaetze.csv`
- GVA exports all available geo datasets every morning into `{File Server Root}\PD\PD-StatA-FST-OGD-Data-GVA\ogd_datensaetze.csv`.
- Open in Excel and find the dataset to be published as OGD.
  Copy contens of column "kontakt_dienststelle" into clipboard. 
- Copy contents of column "ordnerpfad" into clipboard. 

### Define Column "Publizierende Organisation"
- Open File `Publizierende_organisation.csv` located in `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\GVA` in Excel.
- Search worksheet for the value of "kontakt_dienststelle" in clipboard. 
- If not found, add a new row that defines the top-level organisation of the "kontakt_dienststelle" of the dataset to be published.
- "herausgeber" is the responsible "Dienststelle"
- Save, check for unwanted changes using a diff tool, fix if necessary. 

### Fill out file `Metadata.xlsx`
- Open file `Metadata.xlsx` located in `{File Server Root}\PD\PD-StatA-FST-OGD-DataExch\StatA\harvesters\GVA` in Excel.
- Add a new row, paste contents of column "ordnerpfad" copied from the selected row in File `ogd_datensaetze.csv`. 
- Set "import" to "True". 
- Column "shapes": Define which shp files shape(s) should be imported. Leave empty to import all shapes to explore the shapes in ODS before publication. Each shape will be imported as a new ODS dataset. Do not add file extension. Multiple shapes can be separated with semicolon. Do not add a semicolon at the end of a list of shape names. If empty, all shapes will be imported. 
- Column "title_nice": Replace shape names as title of ODS datasets. Multiple entries are separated with semicolon. If empty, shape name is used. If one shape gets a title_nice, all shapes must get a title_nice. 
- Column "ods_id": Dataset id that will be used in ODS. Currently, this id is not automatically set and is just used for reference. 
- Column "beschreibung": Add a description text for the shape(s) in question. If no description is given, the description by GVA is used. 
- Column "referenz": Add URL that will be set as "Reference" in ODS. If left empty, this schould be filled out automatically as "https://geo.bs.ch/...".
- Column "theme": ODS / opendata.swiss theme(s) in German.
- Column "keyword": Semicolon-separated list of keywords to be used in ODS.
- Column "dcat_ap_ch.domain": Used if the dataset should be assigned to an opendata.swiss suborganisation. 
- Column "dcat.accrualperiodicity": Accrual periodicity as described [here](https://handbook.opendata.swiss/de/content/glossar/bibliothek/dcat-ap-ch.html?highlight=accrual)
- Column "schema_file": Set "True" if a (schema file)[[https://help.opendatasoft.com/platform/en/publishing_data/02_harvesting_a_catalog/harvesters/ftp_with_meta_csv.html#schema-csv-file]](https://userguide.opendatasoft.com/l/en/article/wsyubsjp1m-ftp-with-meta-csv-harvester#schema_csv_file) is provided in the [schema_files](./schema_files/) folder. Schema files must be named `{ods_id}.csv`.
- Column "create_map_urls": Set "True" if links to various map services for the specified coordinates should be provided. Example: https://opendatabs.github.io/map-links/?lat=47.564901&lon=7.615269
- Column "dcat.issued": Date string in the form "JJJJ-MM-TT" to be used as issued date in ODS and opendata.swiss.
- Column "tags": If left empty, just opendata.swiss will be filled as tag. Recommended if several datasets with same topic are published.

### Deployment and harvesting
- If schema_file is added or changed, the Airflow Job 'stata_pull_changes' also has to be run.
- Start Airflow Job `gva-geodatenshop`. Shapes are uploaded to FTP, and ODS harvester is started.
- After successful finish of ODS harvester: In Backoffice, check newly created dataset(s), change metadata in file `Metadata.xlsx` accordingly.
- Manually change ODS id of newly datasets. To do this, you have to depublish the dataset first.
- Newly created datasets are not auto-published, but remain private until published in ODS. 
- Changes in datasets that have been published in ODS before are automatically published when the ODS harvester has finished running.
- Repeat until happy with the results ;-)

### Inform Key People at [https://geo.bs.ch](https://geo.bs.ch) and Data Owners
- Contact the relevant key person at [https://geo.bs.ch](https://geo.bs.ch) (via phone/Teams or in person) about our intent to publish some of the data harvested from their portal and the planned publication date on [https://data.bs.ch](https://data.bs.ch). 
- Discuss whether they will inform the respective data owners about the upcoming publication of their data on [https://data.bs.ch](https://data.bs.ch), or whether we should do this ourselves. By default, we do this ourselves via (phone/Teams or in person). 

### Publishing Dataset
- Follow standard procedures to prepare dataset(s) for publication in ODS
