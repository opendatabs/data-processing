# . /BVB/data/etl.sh

cd /BVB/vdv2pg
pip install -r requirements.txt
pip install dist/vdv2pg-0.0.2-py3-none-any.whl

# service postgresql start
pg_ctlcluster 11 main start
sudo -u postgres createuser root
sudo -u postgres psql -c "create database vdv_imports"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE vdv_imports TO root;"
sudo -u postgres psql -c "ALTER ROLE root WITH SUPERUSER;"
sudo -u postgres psql -c "ALTER ROLE postgres WITH SUPERUSER;"
# sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA vdv TO root;"

# su - postgres 
# createdb vdv_imports 
vdv2pg --schema=vdv postgresql:///vdv_imports /BVB/data/b19121801/*.x10
# vdv2pg --schema=vdv --post_ingest_script=/BVB/data/export.sql postgresql:///vdv_imports /BVB/data/b19121801/*.x10

rm -rf /tmp
sudo -u postgres psql -f /BVB/data/connect_export.sql

# sudo -u postgres psql -f /BVB/data/export.sql
# sudo -u postgres psql -f /BVB/data/export_noschema.sql

# sudo -u postgres psql -c "COPY basis_ver_gueltigkeit TO '/BVB/data/csv/basis_ver_gueltigkeit.csv' DELIMITER ';' CSV HEADER;"
# sudo -u postgres psql -c "COPY basis_ver_gueltigkeit TO '/BVB/data/csv/basis_ver_gueltigkeit.csv' DELIMITER ';' CSV HEADER;"
