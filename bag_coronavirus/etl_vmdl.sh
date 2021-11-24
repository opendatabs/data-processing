cd /code/data-processing || exit
python3 -m bag_coronavirus.src.vmdl && python3 -m bag_coronavirus.src.etl_vmdl_impf_uebersicht && python3 -m bag_coronavirus.src.etl_vmdl_altersgruppen && python3 -m bag_coronavirus.src.etl_vmdl_impftyp
