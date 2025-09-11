git config --global --add safe.directory /code/data-processing
git config --global --add safe.directory /code/dags-airflow2
git config --global --add safe.directory /code/rsync
git config --global --add safe.directory /code/R-data-processing/tourismusdashboard
git config --global --add safe.directory /code/R-data-processing/stata_konoer
git config --global --add safe.directory /code/R-data-processing/stata_erwarteter_stromverbrauch
git config --global --add safe.directory /code/R-data-processing/stata_erwarteter_gasverbrauch
# Assumes both data-processing and dags-airflow2 are in the same directory
cd /code/data-processing
git pull
cd /code/dags-airflow2
git pull
cd /code/rsync
git pull
cd /code/R-data-processing/tourismusdashboard
git pull
cd /code/R-data-processing/stata_konoer
git pull
cd /code/R-data-processing/stata_erwarteter_stromverbrauch
git pull
cd /code/R-data-processing/stata_erwarteter_gasverbrauch
git pull
