cd /code/data-processing || exit
# pip freeze > /code/data-processing/kapo_geschwindigkeitsmonitoring/requirements-in-docker.txt
python3 -m kapo_geschwindigkeitsmonitoring.etl

