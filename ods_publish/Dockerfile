FROM python:3.7.6
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["python3", "-m", "ods_publish.etl"]


# Docker commands to create image and run container:
# cd ods_publish
# docker build -t ods-publish .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ods-publish ods-publish python3 -m ods_publish.etl da_20e9bc,da_uxt6fk,da_q78iuw,da_reclv8

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd ods_publish
# docker build -t ods-publish --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name ods-publish ods-publish python3 -m ods_publish.etl da_20e9bc,da_uxt6fk,da_q78iuw,da_reclv8


