FROM python:3.9.1
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["python3", "-m", "ods_catalog.etl"]


# Docker commands to create image and run container:
# cd ods_catalog
# docker build -t ods-ods_catalog .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ods-ods_catalog ods_catalog python3 -m ods_catalog.etl

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd ods_catalog
# docker build -t ods_catalog --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name ods_catalog ods_catalog python3 -m ods_catalog.etl


