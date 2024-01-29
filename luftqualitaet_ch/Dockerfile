FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir pytest==8.0.0rc2
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
CMD ["python3", "-m", "luftqualitaet_ch.src.etl"]


# Docker commands to create image and run container:
# cd luftqualitaet_ch
# docker build -t luftqualitaet_ch .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name luftqualitaet_ch luftqualitaet_ch

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd luftqualitaet_ch
# docker build -t luftqualitaet_ch --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name luftqualitaet_ch luftqualitaet_ch
