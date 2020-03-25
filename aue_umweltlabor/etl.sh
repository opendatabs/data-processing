cd /code/data-processing || exit
# pip freeze > /code/data-processing/aue_umweltlabor/requirements-in-docker.txt
# python3 -m aue_umweltlabor.etl no_file_copy
python3 -m aue_umweltlabor.etl

