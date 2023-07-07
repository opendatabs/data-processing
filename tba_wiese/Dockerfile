FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
CMD ["python3", "-m", "tba_wiese.etl"]


# Docker commands to create image and run container:
# cd tba_wiese
# docker build -t tba_wiese .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name tba_wiese tba_wiese

# Docker on Mac:
# docker run -it --rm -v /Users/jonasbieri/PycharmProjects/data-processing:/code/data-processing --name tba_wiese tba_wiese

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd tba_wiese
# docker build -t tba_wiese --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name tba_wiese tba_wiese


