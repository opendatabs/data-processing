cd /code/data-processing || exit
python3 -m bag_coronavirus.src.copy_bag_datasets && python3 -m bag_coronavirus.etl_test
