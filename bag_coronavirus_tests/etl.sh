cd /code/data-processing || exit
# pip freeze > /code/data-processing/bag_coronavirus_tests/requirements-in-docker.txt
python3 -m bag_coronavirus_tests.etl && python3 -m bag_coronavirus_tests.etl_hosp_capacity

