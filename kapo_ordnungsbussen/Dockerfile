FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
RUN python3 -m pip install --user --no-cache-dir pytest==8.0.0rc2
RUN python3 -m pip install --user --no-cache-dir geopy==2.4.1
RUN python3 -m pip install --user --no-cache-dir rapidfuzz==3.6.1
RUN python3 -m pip install --user --no-cache-dir geopandas==0.14.2
RUN python3 -m pip install --user --no-cache-dir fiona==1.9.4 # geopandas dependency
RUN python3 -m pip install --user --no-cache-dir shapely==2.0.2
CMD ["python3", "-m", "kapo_ordnungsbussen.src.etl"]


# Docker commands to create image and run container:
# cd kapo_ordnungsbussen
# docker build -t kapo_ordnungsbussen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/KaPo/Ordnungsbussen:/code/data-processing/kapo_ordnungsbussen/data_orig --name kapo_ordnungsbussen kapo_ordnungsbussen

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd kapo_ordnungsbussen
# docker build -t kapo_ordnungsbussen --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/KaPo/Ordnungsbussen":/code/data-processing/kapo_ordnungsbussen/data_orig --name kapo_ordnungsbussen kapo_ordnungsbussen
