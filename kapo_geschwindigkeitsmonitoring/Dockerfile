FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY

# Install Spatialite packages (and clean up apt caches)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        spatialite-bin \
        libsqlite3-mod-spatialite && \
    rm -rf /var/lib/apt/lists/*

# Install  Python libraries
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir psycopg2==2.9.9
RUN python3 -m pip install --user --no-cache-dir charset-normalizer==3.3.2
RUN python3 -m pip install --user --no-cache-dir pytest==8.0.0
CMD ["python3", "-m", "kapo_geschwindigkeitsmonitoring.src.etl"]


# Docker commands to create image and run container:
# cd kapo_geschwindigkeitsmonitoring
# docker build -t kapo_geschwindigkeitsmonitoring .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig -v /data/dev/workspace/data-processing:/code/data-processing  --name kapo_geschwindigkeitsmonitoring kapo_geschwindigkeitsmonitoring
