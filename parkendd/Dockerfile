FROM python:3.7.6
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["/bin/bash", "/code/data-processing/parkendd/etl.sh"]

# docker build -t meteoblue .
# docker run -it --rm --mount type=bind,source=$(pwd)/data,target=/data --name meteoblue meteoblue


# Docker commands to create image and run container:
# cd parkendd
# docker build -t parkendd .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name parkendd parkendd

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd parkendd
# docker build -t parkendd --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name parkendd parkendd
