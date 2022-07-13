cd /code/data-processing || exit
# pip freeze > /code/data-processing/mobilitaet_verkehrszaehldaten/requirements-in-docker.txt
# python3 -m mobilitaet_verkehrszaehldaten.etl no_file_copy
python3 -m mobilitaet_verkehrszaehldaten.etl




# commands to be used on old processing server:
## scl enable rh-python36 bash
#cd ~/dev/workspace/mobilitaet_verkehrszaehldaten
#. ../../venv/mobilitaet_verkehrszaehldaten/bin/activate
#python3 etl.py
#deactivate
