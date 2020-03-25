cd /code/data-processing || exit
# python3 -m aue_umweltlabor.etl no_file_copy
python3 -m aue_umweltlabor.etl
pip freeze > /code/data-processing/requirements-in-docker.txt

