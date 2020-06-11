cd /code/data-processing || exit
# pip freeze > /code/data-processing/gva_geodatenshop/requirements-in-docker.txt
python3 -m gva_geodatenshop.etl


# scl enable rh-python36 bash
#cd ~/dev/workspace/GVA-Geodatenshop
#. ../../venv/GVA-Geodatenshop/bin/activate
#python3 etl.py
#deactivate


