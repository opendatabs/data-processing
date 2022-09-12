FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.7
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir dateparser==1.0.0
RUN python3 -m pip install --user --no-cache-dir pytest==6.2.4
CMD ["/bin/bash", "/code/data-processing/staka_briefliche_stimmabgaben/etl.sh"]


# Docker commands to create image and run container:
# cd staka_briefliche_stimmabgaben
# docker build -t staka_briefliche_stimmabgaben .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/staka-abstimmungen:/code/data-processing/staka_briefliche_stimmabgaben/data --name staka_briefliche_stimmabgaben staka_briefliche_stimmabgaben

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd staka_briefliche_stimmabgaben
# docker build -t staka_briefliche_stimmabgaben --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/staka-abstimmungen":/code/data-processing/staka_briefliche_stimmabgaben/data  --name staka_briefliche_stimmabgaben staka_briefliche_stimmabgaben


