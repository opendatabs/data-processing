# OGD Data Processing Basel-Stadt
Source code to process data to be published as Open Government Data (OGD) for Canton Basel-Stadt, Switzerland

## Overview
### Involved Servers
The Open Data infrastructure of Basel-Stadt consists of the following platforms:
- Data Processing Server (internal)
- Web Server https://data-bs.ch
- Data Platform  https://data.bs.ch

### Outline of the ETL Process
Data is regularly published from data-producing governmental entities on internal network drives to the [Fachstelle OGD](https://opendata.bs.ch). From there, jobs running on the data processing server read and _extract_, _transform_ and then _load_ ([ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)) the resulting dataset to the web server via (S)FTP. These datasets are then retrieved and published by the data platform so that they can be consumed by the public. 

### Involved Systems

1. Data processing Server
    - Linux mount points below /mnt that serve the data received from other government entities
    - [Docker](https://en.wikipedia.org/wiki/Docker_(software)) daemon.
    - ETL jobs programmed in [Python](https://en.wikipedia.org/wiki/Python_(programming_language)). Source code of these jobs are in subfolders of the present repository, see e.g. [aue-umweltlabor](https://github.com/opendatabs/data-processing/tree/master/aue_umweltlabor).
    - ETL jobs containerized in Docker images, so that each job has its own containerized environment to run in. The environment is configured using the Dockerfile, see e.g. [here](https://github.com/opendatabs/data-processing/blob/master/aue_umweltlabor/Dockerfile).  
    - [AirFlow](https://en.wikipedia.org/wiki/Apache_Airflow) workflow scheduler. Runs as a docker container, see [configuration](https://github.com/opendatabs/docker-airflow).  
    - Every ETL job to run has its own Apache Airflow [Directed Acyclical Graph (DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph) file. It is written in Python and defines when a containerized ETL job is run, and how to proceed if the job fails. DAG files are stored in the [AirFlow repo](https://github.com/opendatabs/docker-airflow/tree/master/dags), see e.g. [this one](https://github.com/opendatabs/docker-airflow/blob/master/dags/aue-umweltlabor.py).
    
1. General-Purpose Web Server [https://data-bs.ch](https://data-bs.ch)
    - Linux server that is primarly used to host data ready to be published onto the data portal
    - Hosts the [RUES Viz for realtime Rhein data](https://rues.data-bs.ch/onlinedaten/onlinedaten.html), see the [source code](https://github.com/opendatabs/data-bs.ch/tree/master/public_html/rues/onlinedaten). 
    - Hosts [some cron jobs](https://github.com/opendatabs/data-bs.ch/tree/master/cronjobs), those are being trainsitioned to run as AirFlow jobs on the data processing server. 
    - All data on this server is public, including data that is being processed on this server before publication.  

1. Data Platform [https://data.bs.ch](https://data.bs.ch)
    - The data platform is a cloud service that is not hosted on the BS network, but by [Opendatasoft](https://opendatasoft.com). 
    - It presents data to the public in diverse formats (table, file export, Viz, 
    API).
    - Simple processing steps can be applied also here. 
    - All data on this server is public, including data that is being processed on this server before publication.  
    - Data is retrieved from the web server via FTP or HTTPS. Exceptions include the following: 
        - Real time data being pushed into the data platform via [Opendatasoft Real Time API](https://help.opendatasoft.com/platform/en/publishing_data/03_scheduling_updates/scheduling_updates.html#pushing-real-time-data), e.g. [Occupation of charging stations](https://data.bs.ch/explore/dataset/100004)
        - Real time data retrieved every minute directly by the data platform without involvement of any AirFlow jobs on the data processing server, e.g. [Occupation status of parking lots](https://data.bs.ch/explore/dataset/100088)
    
 ### Data Harvesting Mechanisms
 #### Into the Data Platform
 
 1. Single datasets are source via these mechanisms: : 
    - Direct https source, e.g. [Coronavirus Fallzahlen Basel-Stadt](https://data.bs.ch/explore/dataset/100073)
    - Opendatasoft realtime API, e.g. [Real-time occupation status of chargig stations](https://data.bs.ch/explore/dataset/100004)
    - FTP(S) sourcing of a directory, e.g. [Smart Climate Schallpegelmessungen](https://data.bs.ch/explore/dataset/100087)
 
     About using a directory instead of a file as the source for a dataset: "Using a directory is often the prefered solution to automate incremental updates between a customer's information system and the platform. All the files in the directory need to have the same format and schema (e.g. CSV files with the same column titles). In case of automation, whenever the dataset is published, new and updated files are fetched from the remote location and processed and thanks to Opendatasoft's native deduplication strategy". For more technical information how these mechanisma work see the [Opendatasoft documentation](https://help.opendatasoft.com/platform/en/publishing_data/01_creating_a_dataset/sourcing_data.html#sourcing-remote-data-via-a-url).
    
 1. Catalogs of datasets are harvested via the [FTP with meta CSV harvester](https://help.opendatasoft.com/platform/en/publishing_data/02_harvesting_a_catalog/harvesters/ftp_with_meta_csv.html). Currently th
 
 #### Out of the Data Platform