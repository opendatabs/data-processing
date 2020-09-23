FROM python:3.7.6
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
# RUN python3 -m pip install --user --no-cache-dir pandas
RUN python3 -m pip install --user --no-cache-dir requests
CMD ["/bin/bash", "/code/data-processing/stata_daily_upload/etl.sh"]


# Docker commands to create image and run container:
# cd stata_daily_upload
# docker build -t stata_daily_upload .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/Bevoelkerung:/code/data-processing/stata_daily_upload/data -v /data/dev/workspace/data-processing:/code/data-processing --name stata_daily_upload stata_daily_upload

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd stata_daily_upload
# docker build -t stata_daily_upload --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name stata_daily_upload stata_daily_upload


