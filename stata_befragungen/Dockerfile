FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir charset_normalizer==3.3.2
CMD ["python3", "-m", "stata_befragungen.src.etl"]


# Docker commands to create image and run container:
# cd stata_befragungen
# docker build -t stata_befragungen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/StatA/Befragungen:/code/data-processing/stata_befragungen/data_orig --name stata_befragungen stata_befragungen

# Docker on Mac:
# docker run -it --rm -v /Users/jonasbieri/PycharmProjects/data-processing:/code/data-processing --name stata_befragungen stata_befragungen

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd stata_befragungen
# docker build -t stata_befragungen --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name stata_befragungen stata_befragungen
