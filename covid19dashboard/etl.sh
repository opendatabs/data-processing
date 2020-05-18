cd /code/data-processing || exit
# pip freeze > /code/data-processing/covid19dashboard/requirements-in-docker.txt
python3 -m covid19dashboard.etl

