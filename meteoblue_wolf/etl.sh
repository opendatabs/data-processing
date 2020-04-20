cd /code/data-processing || exit
# pip freeze > /code/data-processing/meteoblue_wolf/requirements-in-docker.txt
python3 -m meteoblue_wolf.etl

