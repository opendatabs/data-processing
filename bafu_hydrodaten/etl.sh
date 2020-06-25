cd /code/data-processing || exit
# pip freeze > /code/data-processing/aue_schall/requirements-in-docker.txt
python3 -m bafu_hydrodaten.etl

