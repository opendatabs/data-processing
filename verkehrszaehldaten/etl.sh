cd /code/data-processing || exit
# pip freeze > /code/data-processing/verkehrszaehldaten/requirements-in-docker.txt
python3 -m verkehrszaehldaten.etl



# commands to be used on old processing server:
## scl enable rh-python36 bash
#cd ~/dev/workspace/verkehrszaehldaten
#. ../../venv/verkehrszaehldaten/bin/activate
#python3 etl.py
#deactivate
