git config --global --add safe.directory /code/data-processing
git config --global --add safe.directory /code/dags-airflow2
git config --global --add safe.directory /code/startercode-generator-bs
# Assumes both data-processing, dags-airflow2 and startercode-generator-bs are in the same directory
cd /code/data-processing
git pull
cd /code/dags-airflow2
git pull
cd /code/startercode-generator-bs
git pull