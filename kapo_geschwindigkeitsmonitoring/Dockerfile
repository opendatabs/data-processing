FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir psycopg2==2.9.9
RUN python3 -m pip install --user --no-cache-dir charset-normalizer==3.3.2
RUN python3 -m pip install --user --no-cache-dir pytest==8.0.0
CMD ["python3", "-m", "kapo_geschwindigkeitsmonitoring.etl"]


# Docker commands to create image and run container:
# cd kapo_geschwindigkeitsmonitoring
# docker build -t kapo_geschwindigkeitsmonitoring .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig -v /data/dev/workspace/data-processing:/code/data-processing  --name kapo_geschwindigkeitsmonitoring kapo_geschwindigkeitsmonitoring

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd kapo_geschwindigkeitsmonitoring
# docker build -t kapo_geschwindigkeitsmonitoring --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing\kapo_geschwindigkeitsmonitoring\data_orig":/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig -v "C:\dev\workspace\data-processing":/code/data-processing --name kapo_geschwindigkeitsmonitoring kapo_geschwindigkeitsmonitoring
