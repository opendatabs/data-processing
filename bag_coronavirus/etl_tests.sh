cd /code/data-processing || exit
python3 -m bag_coronavirus.copy_bag_datasets && python3 -m bag_coronavirus.etl_test
