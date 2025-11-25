# Open Government Data Processing Basel-Stadt
Architecture, processes, methods and code used to process Open Government Data (OGD) for Canton Basel-Stadt, Switzerland. 

## Overview of Architecture and Processes
### Involved Servers
The Open Data infrastructure of Basel-Stadt consists of the following platforms:
- Data Processing Server (internal)
- Web Server https://data-bs.ch
- Data Platform  https://data.bs.ch

### Outline of the ETL Process
Usually, data is published from data-producing governmental entities on internal network drives to the [Fachstelle OGD](https://data.bs.ch). From there, jobs running on the data processing server read and _extract_, _transform_ and then _load_ ([ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)) the resulting dataset to the web server via (S)FTP. These datasets are then retrieved and published by the data platform so that they can be consumed by the public. 

## Technical Implementation
### Involved Systems and their purpose

1. Data Processing Server (internal)
    - There are Linux mount points below the folder "/mnt" that serve the data received from other government entities.
    - It runs [Docker](https://en.wikipedia.org/wiki/Docker_(software)) daemon which hosts docker containers that each contain their own isolated data transformation job.
    - ETL jobs programmed in [Python](https://en.wikipedia.org/wiki/Python_(programming_language)). Source code of these jobs are in subfolders of the present repository, see e.g. [aue-umweltlabor](https://github.com/opendatabs/data-processing/tree/main/aue_umweltlabor).
    - ETL jobs containerized in Docker images, so that each job has its own containerized environment to run in. The environment is configured using the Dockerfile, see e.g. [here](https://github.com/opendatabs/data-processing/blob/main/aue_umweltlabor/Dockerfile).  
    - [AirFlow](https://en.wikipedia.org/wiki/Apache_Airflow) workflow scheduler. Runs as a docker container, see [configuration](https://github.com/opendatabs/docker-airflow).  
    - Every ETL job to run has its own Apache Airflow [Directed Acyclical Graph (DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph) file. It is written in Python and defines when a containerized ETL job is run, and how to proceed if the job fails. DAG files are stored in the [AirFlow repo](https://github.com/opendatabs/docker-airflow/tree/master/dags), see e.g. [this one](https://github.com/opendatabs/docker-airflow/blob/master/dags/aue-umweltlabor.py).
    - AirFlow DAG jobs can be found on the server in the folder '/data/dev/workspace/docker-airflow/dags', ETL jobs in '/data/dev/workspace/data-processing'.
    - Deployment of source code is done via git: Push from development environment to github, pull from github to live environment in above mentioned folders.  
    
1. Web Server https://data-bs.ch
    - Linux server that is primarly used to host data ready to be published onto the data portal.
    - Hosts the [RUES Viz for real-time Rhein data](https://rues.data-bs.ch/onlinedaten/onlinedaten.html), see the [source code](https://github.com/opendatabs/data-bs.ch/tree/master/public_html/rues/onlinedaten). 
    - Hosts [some cron jobs](https://github.com/opendatabs/data-bs.ch/tree/master/cronjobs), those are being trainsitioned to run as AirFlow jobs on the data processing server. 
    - All data on this server is public, including data that is being processed on this server before publication.  

1. Data Platform https://data.bs.ch
    - The data platform is a cloud service that is not hosted on the BS network, but by [Opendatasoft](https://opendatasoft.com). 
    - It presents data to the public in diverse formats (table, file export, Viz, 
    API).
    - Simple processing steps can be applied also here. 
    - All data on this server is public, including data that is being processed on this server before publication.  
    - Data is retrieved from the web server via FTP or HTTPS. Exceptions include the following: 
        - Real-time data being pushed into the data platform via [Opendatasoft Real Time API](https://help.opendatasoft.com/platform/en/publishing_data/03_scheduling_updates/scheduling_updates.html#pushing-real-time-data), e.g. [Occupation of charging stations](https://data.bs.ch/explore/dataset/100004)
        - Real-time data retrieved every minute directly by the data platform without involvement of any AirFlow jobs on the data processing server, e.g. [Occupation status of parking lots](https://data.bs.ch/explore/dataset/100088)
    
 ### Data Harvesting Mechanisms
 #### Into the Data Platform
 
 1. Single datasets are sourced via these mechanisms: 
    - Direct https source, e.g. [Coronavirus Fallzahlen Basel-Stadt](https://data.bs.ch/explore/dataset/100073)
    - Opendatasoft real-time API, e.g. [Real-time occupation status of charging stations](https://data.bs.ch/explore/dataset/100004)
    - FTP(S) sourcing of a directory, e.g. [Smart Climate Schallpegelmessungen](https://data.bs.ch/explore/dataset/100087)
 
     About using an FTP(S) directory instead of a file as the source for a dataset: "Using a directory is often the prefered solution to automate incremental updates between a customer's information system and the platform. All the files in the directory need to have the same format and schema (e.g. CSV files with the same column titles). In case of automation, whenever the dataset is published, new and updated files are fetched from the remote location and processed and thanks to Opendatasoft's native deduplication strategy". For more technical information how these mechanisms work see the [Opendatasoft documentation](https://help.opendatasoft.com/platform/en/publishing_data/01_creating_a_dataset/sourcing_data.html#sourcing-remote-data-via-a-url).
    
 1. Catalogs of datasets are harvested via the [FTP with meta CSV harvester](https://help.opendatasoft.com/platform/en/publishing_data/02_harvesting_a_catalog/harvesters/ftp_with_meta_csv.html). Currently these include the following: 
    1. OGD datasets by Statistisches Amt Basel-Stadt
        - Metadata of datasets to be harvested by the data portal are saved onto the web server in folder "/public_html/opendatasoft/harvesters/stata/ftp-csv/" by the (closed source) publishing process run by members of the Statistisches Amt. 
    1. Open Datasets by Grundbuch- und Vermessungsamt Basel-Stadt
        - Data and metadata of datasets to be harvested by the data platform are daily created by the data processing job [gva_geodatenshop](https://github.com/opendatabs/data-processing/blob/main/gva_geodatenshop/etl.py) and uploaded to the web server into  folder "/public_html/opendatasoft/harvesters/GVA/". 
 
 #### Out of the Data Platform
 The data platform can be harvested by other data platforms e.g. via the [DCAT-AP for Switzerland API](https://www.ech.ch/de/standards/39919) by using an URL in the form of [https://data.bs.ch/api/v2/catalog/exports/dcat_ap_ch](https://data.bs.ch/api/v2/catalog/exports/dcat_ap_ch) (see [here](https://help.opendatasoft.com/apis/ods-search-v2/#exporting-datasets) for further technical information).  
 
 To our knowledge, the only direct current consumer/harvester of our data platform metadata is https://opendata.swiss, which in turn is being harvested by the [European Data Portal](https://www.europeandataportal.eu/), and possibly others. 
 
 As an example, see how this dataset presented by different data portals:
 - In the data portal Basel-Stadt (original): https://data.bs.ch/explore/dataset/100042
 - In opendata.swiss (harvested from the above): https://opendata.swiss/de/dataset/statistische-raumeinheiten-wohnviertel
 - In the European Data portal (harvested from the above): https://www.europeandataportal.eu/data/datasets/100042-statistisches-amt-kanton-basel-stadt
    
 ### Miscellaneous
 #### Usage of git
 - On the data processing server we use the Docker container 'alpine/git:v2.26.2' as a git client, see https://hub.docker.com/r/alpine/git. 
 - First usage on the Docker host to download the Docker image and see `git --version`executed:
~~~
docker run -ti --rm -v ${HOME}:/root -v $(pwd):/git alpine/git:v2.26.2 --version
 ~~~
 - Adding a custom 'git' function in ~/.bashrc: 
~~~
# User specific aliases and functions
function git () {
    (docker run -ti --rm -v ${HOME}:/root -v $(pwd):/git alpine/git:v2.26.2 "$@")
}
~~~ 
 

#### Embargo Feature
- To create an embargo on a dataset based on a csv file named "data.csv", place a file named "data_embargo.txt" into the folder where the data file resides. 
- The "_embargo.txt" file must contain a datetime string in the form YYYY-MM-DDThh:mm, e.g.
~~~
2021-10-22T09:00
 ~~~
- The data processing job must be enhanced to use the embargo function:
~~~
common.is_embargo_over(data_file_path)
~~~
- Always update the embargo file before uploading new data!
