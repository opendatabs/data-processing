cd /code/data-processing || exit
# pip freeze > /code/data-processing/covid19bs/requirements-in-docker.txt
python3 -m covid19bs.etl

