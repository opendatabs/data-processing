FROM python:3.10.0
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
CMD ["/bin/bash", "/code/data-processing/gd_coronavirus_abwassermonitoring/etl.sh"]


# Docker commands to create image and run container:
# cd gd_coronavirus_abwassermonitoring
# docker build -t gd_coronavirus_abwassermonitoring .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name gd_coronavirus_abwassermonitoring gd_coronavirus_abwassermonitoring

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd gd_coronavirus_abwassermonitoring
# docker build -t gd_coronavirus_abwassermonitoring --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  --name gd_coronavirus_abwassermonitoring gd_coronavirus_abwassermonitoring
