FROM python:3.11-bullseye

# 1) Install SpatiaLite dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        spatialite-bin \
        libsqlite3-mod-spatialite && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /code/data-processing

# 2) Install your existing Python libraries
RUN python3 -m pip install --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --no-cache-dir requests==2.31.0
RUN python3 -m pip install --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --no-cache-dir psycopg2==2.9.9
RUN python3 -m pip install --no-cache-dir charset-normalizer==3.3.2
RUN python3 -m pip install --no-cache-dir pytest==8.0.0

# 3) Install Datasette
RUN python3 -m pip install --no-cache-dir datasette

# 4) Run script after installing extension using Datasette
CMD ["datasette", "--load-extension=/usr/lib/x86_64-linux-gnu/mod_spatialite.so", "--help"]
CMD ["python3", "-m", "kapo_geschwindigkeitsmonitoring.src.etl"]

# Docker commands to create image and run container:
# cd kapo_geschwindigkeitsmonitoring
# docker build -t kapo_geschwindigkeitsmonitoring .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig -v /data/dev/workspace/data-processing:/code/data-processing  --name kapo_geschwindigkeitsmonitoring kapo_geschwindigkeitsmonitoring
