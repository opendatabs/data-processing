### Updating process

The AirFlow DAG job iwb_netzlast.py (in '/data/dev/workspace/docker-airflow/dags/')
runs every hour and updates the dataset if it finds new data in the folder:

'...\PD-StatA-FST-OGD-DataExch\IWB\Netzlast\latest_data'

When new data arrives by email move it to this folder so that the dataset will be automatically updated. 