cd /code/data-processing || exit
python3 -m covid19bs.etl_bs && python3 -m covid19bs.src.etl_copy_files

