# Harvester for GVA Gedodata files

## Publish existing GVA datasets
### Identify dataset to publish in ogd_datensaetze.csv
- GVA exports all available geo datasets every morning into `{File Server Root}\PD\PD-StatA-FST-OGD-Data-GVA\ogd_datensaetze.csv`.
- Open in Excel and find the dataset to be published as OGD.
  Copy contens of column "kontakt_dienststelle" into clipboard. 
- Copy contents of column "ordnerpfad" into clipboard. 

### Define Column "Publizierende Organisation"
- Open File [Publizierende_organisation.csv](./Publizierende_organisation.csv) in Excel.
- Search worksheet for the value of "kontakt_dienststelle" in clipboard. 
- If not found, add a new row that defines the top-level organisation of the "kontakt_dienststelle" of the dataset to be published.
- "herausgeber" is the second keyword after the top-level-organisation.
- Save, check for unwanted changes using a diff tool, fix if necessary. 
- Commit, push to github. 

### Fill out file Metadata.csv
- Open file [Metadata.csv](./Metadata.csv) in Excel. If you use a different editor, make sure you use Encoding "Windows-1252" to edit and save file.
- Reformat column "dcat.issued" in Excel to use custom format "JJJJ-MM-TT".
- Add a new row, paste contents of column "ordnerpfad" copied from the selected row in File `ogd_datensaetze.csv`. 
- Set "import" to "True". 
- Column "shapes": Define which shp files shape(s) should be imported. Leave empty to import all shapes to explore the shapes in ODS before publication. Each shape will be imported as a new ODS dataset. Do not add file extension. Multiple shapes can be separated with semicolon. Do not add a semicolon at the end of a list of shape names. If empty, all shapes will be imported. 
- Column "title_nice": Replace shape names as title of ODS datasets. Multiple entries are separated with semicolon. If empty, shape name is used. If one shape gets a title_nice, all shapes must get a title_nice. 
- Column "ods_id": Dataset id that will be used in ODS. Currently, this id is not automatically set and is just used for reference. 
- Column "beschreibung": Add a description text for the shape(s) in question. If no description is given, the description by GVA is used. 
- Column "referenz": Add URL that will be set as "Reference" in ODS. Currently this is used to add links to "https://models.geo.bs.ch/..." (which is planned to be imported automatically in the future). 
- Column "theme": ODS / opendata.swiss theme(s) in German. 
- Column "keyword": Semicolon-separated list of keywords to be used in ODS.
- Column "dcat_ap_ch.domain": Used if the dataset should be assigned to an opendata.swiss suborganisation. 
- Column "dcat.accrualperiodicity": Accrual periodicity as described [here](https://handbook.opendata.swiss/de/content/glossar/bibliothek/dcat-ap-ch.html?highlight=accrual)
- Column "schema_file": Set "True" if a (schema file)[https://help.opendatasoft.com/platform/en/publishing_data/02_harvesting_a_catalog/harvesters/ftp_with_meta_csv.html#schema-csv-file] is provided in the current folder. Schema files must be named `{ods_id}.csv`. 
- Column "dcat.issued": Date string in the form "JJJJ-MM-TT" to be used as issued date in ODS and opendata.swiss.
- Make sure you use Encoding "Windows-1252" to save file.
  
### Deployment and harvesting
- Check changes in file Metadata.csv using a diff viewer.
- If everything seem fine: Commit file to git repo, push to github. 
- Connect to processing server using ssh, cd to `/data/dev/workspace/data-processing`, issue command `git pull`.
- Start Airflow Job `gva-geodatenshop`. Shapes are uploaded to FTP, and ODS harvester is started.
- After successful finish of ODS harvester: In Backoffice, check newly created dataset(s), change metadata in file `Metadata.csv` accordingly.
- Manually change ODS id of newly datasets. 
- Newly created datasets are not auto-published, but remain private until published in ODS. 
- Changes in datasets that have been published in ODS before are automatically published when the ODS harvester has finished running.
- Repeat until happy with the results ;-)

### Publishing Dataset
- Follow standard procedures to prepare dataset(s) for publication in ODS
