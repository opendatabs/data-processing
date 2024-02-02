FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
CMD ["python3", "-m", "bafu_hydrodaten.etl_https"]


# Docker commands to create image and run container:
# cd bafu_hydrodaten
# docker build -t bafu_hydrodaten .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing  --name bafu_hydrodaten bafu_hydrodaten

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd bafu_hydrodaten
# docker build -t bafu_hydrodaten --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name bafu_hydrodaten bafu_hydrodaten
