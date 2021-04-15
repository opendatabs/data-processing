FROM python:3.9.0
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.25.1
RUN python3 -m pip install --user --no-cache-dir xlrd==1.2.0
CMD ["/bin/bash", "/code/data-processing/euroairport/etl.sh"]

# docker build -t euroairport .
# docker run -it --rm --mount type=bind,source=$(pwd)/data,target=/data --name euroairport euroairport


# Docker commands to create image and run container:
# cd euroairport
# docker build -t euroairport .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/EuroAirport:/code/data-processing/euroairport/data  --name euroairport euroairport

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd euroairport
# docker build -t euroairport --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name euroairport euroairport
