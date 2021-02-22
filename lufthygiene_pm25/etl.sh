cd /code/data-processing || exit
# pip freeze > /code/data-processing/lufthygiene_pm25/requirements-in-docker.txt
python3 -m lufthygiene_pm25.etl

