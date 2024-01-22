FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0rc0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir python-dateutil==2.8.2
RUN python3 -m pip install --user --no-cache-dir pycryptodome==3.20.0
CMD ["python3", "-m", "meteoblue_wolf.etl"]

# docker build -t meteoblue .
# docker run -it --rm --mount type=bind,source=$(pwd)/data,target=/data --name meteoblue meteoblue


# Docker commands to create image and run container:
# cd meteoblue_wolf
# docker build -t meteoblue-wolf .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name meteoblue-wolf meteoblue-wolf

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd meteoblue_wolf
# docker build -t meteoblue-wolf --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name meteoblue-wolf meteoblue-wolf
