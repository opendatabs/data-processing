FROM python:3.9.2
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.25.1
CMD ["/bin/bash", "/code/data-processing/tba_abfuhrtermine/etl.sh"]


# Docker commands to create image and run container:
# cd tba_abfuhrtermine
# docker build -t tba_abfuhrtermine .
# cd ..
# docker run -it --rm -v /mnt/OGD-GVA:/code/data-processing/tba_abfuhrtermine/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name tba_abfuhrtermine tba_abfuhrtermine

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd tba_abfuhrtermine
# docker build -t tba_abfuhrtermine --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "\\bs.ch\dfs\BS\PD\PD-StatA-FST-OGD-Data-GVA":/code/data-processing/tba_abfuhrtermine/data_orig -v "C:\dev\workspace\data-processing":/code/data-processing --name tba_abfuhrtermine tba_abfuhrtermine
