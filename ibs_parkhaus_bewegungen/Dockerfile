FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir xlrd==1.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
CMD ["python3", "-m", "ibs_parkhaus_bewegungen.etl"]


# Docker commands to create image and run container:
# cd ibs_parkhaus_bewegungen
# docker build -t ibs_parkhaus_bewegungen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/FD-immobilienbs/2022-06-09:/code/data-processing/ibs_parkhaus_bewegungen/data_orig --name ibs_parkhaus_bewegungen ibs_parkhaus_bewegungen

# Docker on Mac:
# docker run -it --rm -v /Users/jonasbieri/PycharmProjects/data-processing:/code/data-processing --name ibs_parkhaus_bewegungen ibs_parkhaus_bewegungen

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd ibs_parkhaus_bewegungen
# docker build -t ibs_parkhaus_bewegungen --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name ibs_parkhaus_bewegungen ibs_parkhaus_bewegungen


