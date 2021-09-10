FROM python:3.9.2
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.25.1
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5

CMD ["/bin/bash", "/code/data-processing/covid19bs/etl.sh"]


# Docker commands to create image and run container:
# cd covid19bs
# docker build -t covid19bs .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/MD/upload:/code/data-processing/covid19bs/data_orig --name covid19bs covid19bs

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd covid19bs
# docker build -t covid19bs --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  -v /mnt/OGD-DataExch/MD/ogd_upload:/code/data-processing/covid19bs/data --name covid19bs covid19bs
