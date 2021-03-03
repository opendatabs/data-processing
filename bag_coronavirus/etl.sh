cd /code/data-processing || exit
python3 -m bag_coronavirus.etl_test && python3 -m bag_coronavirus.etl_hosp_capacity

