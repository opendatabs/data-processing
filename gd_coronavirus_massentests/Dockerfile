FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.7
RUN python3 -m pip install --user --no-cache-dir pandasql==0.7.3
RUN python3 -m pip install --user --no-cache-dir pytest==6.2.4
CMD ["python3", "-m", "gd_coronavirus_massentests.src.etl"]


# Docker commands to create image and run container:
# cd gd_coronavirus_massentests
# docker build -t gd_coronavirus_massentests .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/GD-GS/coronavirus-massentests:/code/data-processing/gd_coronavirus_massentests/data --name gd_coronavirus_massentests gd_coronavirus_massentests

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd gd_coronavirus_massentests
# docker build -t gd_coronavirus_massentests --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/GD-GS/coronavirus-massentests":/code/data-processing/gd_coronavirus_massentests/data --name gd_coronavirus_massentests gd_coronavirus_massentests
