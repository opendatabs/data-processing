cd /code/data-processing || exit
# pip freeze > /code/data-processing/staka_wahlen_abstimmungen/requirements-in-docker.txt
python3 -m staka_abstimmungen.src.etl_details && python3 -m staka_abstimmungen.src.etl_kennzahlen

