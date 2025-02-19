FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.1.5
CMD ["python3", "-m", "ods_catalog.etl"]


# Docker commands to create image and run container:
# cd ods_catalog
# docker build -t ods_catalog .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /code/data-processing/data:/mnt/OGD-DataExch/StatA/FST-OGD --name ods-ods_catalog ods_catalog python3 -m ods_catalog.etl
