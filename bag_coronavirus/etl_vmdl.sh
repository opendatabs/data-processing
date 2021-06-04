cd /code/data-processing || exit
python3 -m bag_coronavirus.vmdl && python3 -m bag_coronavirus.etl_vmdl_impf_uebersicht && python3 -m bag_coronavirus.etl_vmdl_altersgruppen
