cd /code/data-processing || exit
python3 -m covid19_hosp.hosp_extract && python3 -m covid19_hosp.hosp_transform


