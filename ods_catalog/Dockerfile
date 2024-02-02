FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
CMD ["python3", "-m", "ods_catalog.etl"]


# Docker commands to create image and run container:
# cd ods_catalog
# docker build -t ods_catalog .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ods-ods_catalog ods_catalog python3 -m ods_catalog.etl

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd ods_catalog
# docker build -t ods_catalog --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name ods_catalog ods_catalog python3 -m ods_catalog.etl


