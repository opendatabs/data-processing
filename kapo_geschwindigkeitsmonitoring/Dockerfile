FROM python:3.9.0
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["/bin/bash", "/code/data-processing/kapo_geschwindigkeitsmonitoring/etl.sh"]


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
