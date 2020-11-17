cd /code/data-processing || exit
# pip freeze > /code/data-processing/md_covid19cases/requirements-in-docker.txt
python3 -m md_covid19cases.etl

