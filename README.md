# OGD Data Processing Basel-Stadt
Source code to process data to be published as Open Government Data (OGD) for Canton Basel-Stadt, Switzerland

## Overview
### Involved Servers
The Open Data infrastructure of Basel-Stadt consists of the following platforms:
- Data Processing Server (internal)
- General-purpose Web Server https://data-bs.ch
- Data Platform for the general public https://data.bs.ch

### Outline of the ETL Process
Data is regularly published from data-producing governmental entities on internal network drives to the [Fachstelle OGD](https://opendata.bs.ch). From there, data processing jobs read and _extract_ the data, _transform_ it, and _load_ ([ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)) the resulting dataset to the web server via (S)FTP. These datasets are then loaded by the data platform so that they can be consumed by the public. 

### Involved Systems
1. Data processing Server
- Linux mount points below /mnt that serve the data received from other government entities
- [Docker](https://en.wikipedia.org/wiki/Docker_(software)) daemon.
- ETL jobs programmed in [Python](https://en.wikipedia.org/wiki/Python_(programming_language)). Source code of these jobs are in subfolders of the present repository, see e.g. [aue-umweltlabor](https://github.com/opendatabs/data-processing/tree/master/aue_umweltlabor).
- ETL jobs containerized in Docker images, so that each job has its own containerized environment to run in. The environment is configured using the Dockerfile, see e.g. [here](https://github.com/opendatabs/data-processing/blob/master/aue_umweltlabor/Dockerfile).  
- [AirFlow](https://en.wikipedia.org/wiki/Apache_Airflow) workflow scheduler. Runs as a docker container, see [configuration](https://github.com/opendatabs/docker-airflow).  
- Every ETL job to run has its own Apache Airflow [Directed Acyclical Graph (DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph) file. It is written in Python and defines when a containerized ETL job is run, and how to proceed if the job fails. DAG files are stored in the [AirFlow repo](https://github.com/opendatabs/docker-airflow/tree/master/dags), see e.g. [this one](https://github.com/opendatabs/docker-airflow/blob/master/dags/aue-umweltlabor.py).
2. General-Purpose Web Server [https://data-bs.ch](https://data-bs.ch)
3. Data Platform [https://data.bs.ch](https://data.bs.ch)
