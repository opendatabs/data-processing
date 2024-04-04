/bin/bash /code/data-processing/stata_pull_changes/set_proxy.sh
git config --global --add safe.directory /code/data-processing
git config --global --add safe.directory /code/dags-airflow2
# Assumes both data-processing and dags-airflow2 are in the same directory
cd /code/data-processing
git pull
cd /code/dags-airflow2
git pull
