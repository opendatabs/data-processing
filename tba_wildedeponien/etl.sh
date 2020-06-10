cd /code/data-processing || exit
# pip freeze > /code/data-processing/tba_wildedeponien/requirements-in-docker.txt
python3 -m tba_wildedeponien.etl

