# scl enable rh-python36 bash
cd ~/dev/workspace/verkehrszaehldaten
. ../../venv/verkehrszaehldaten/bin/activate
python3 etl.py
deactivate
