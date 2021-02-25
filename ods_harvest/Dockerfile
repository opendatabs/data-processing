FROM python:3.9.2
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["python3", "-m", "ods_harvest.etl"]


# Docker commands to create image and run container:
# cd ods_harvest
# docker build -t ods-harvest .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ods-harvest ods-harvest python3 -m ods_harvest.etl gva-ftp-csv

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd ods_harvest
# docker build -t ods-harvest --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name ods-harvest ods-harvest python3 -m ods_harvest.etl gva-ftp-csv


