FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir beautifulsoup4===4.11.1
CMD ["python3", "-m", "covid19_hosp.etl"]


# Docker commands to create image and run container:
# cd covid19_hosp
# docker build -t covid19_hosp .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/MD/ogd_upload:/code/data-processing/covid19_hosp/data --name covid19_hosp covid19_hosp

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd covid19_hosp
# docker build -t covid19_hosp --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing -v /mnt/OGD-DataExch/MD/upload:/code/data-processing/covid19_hosp/data --name covid19_hosp covid19_hosp
