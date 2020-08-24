cd /code/data-processing || exit
# pip freeze > /code/data-processing/tba_abfuhrtermine/requirements-in-docker.txt
python3 -m tba_abfuhrtermine.etl
