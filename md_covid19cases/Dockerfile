FROM python:3.9.0
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["/bin/bash", "/code/data-processing/md_covid19cases/etl.sh"]


# Docker commands to create image and run container:
# cd md_covid19cases
# docker build -t md_covid19cases .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/MD/upload:/code/data-processing/md_covid19cases/data --name md_covid19cases md_covid19cases

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd md_covid19cases
# docker build -t md_covid19cases --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing -v /mnt/OGD-DataExch/MD/upload:/code/data-processing/md_covid19cases/data --name md_covid19cases md_covid19cases
