cd /code/data-processing || exit
# pip freeze > /code/data-processing/euroairport/requirements-in-docker.txt
python3 -m euroairport.etl

