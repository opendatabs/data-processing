FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.27.1
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir pandasql==0.7.3
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.9
CMD ["/bin/bash", "/code/data-processing/bag_coronavirus/etl_bag_datasets.sh"]


# Docker commands to create image and run container:
# cd bag_coronavirus
# docker build -t bag_coronavirus .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/StatA/BAG_Coronavirus_Tests:/code/data-processing/bag_coronavirus/data -v /mnt/OGD-DataExch/MD-HMW:/code/data-processing/bag_coronavirus/vmdl_data --name bag_coronavirus bag_coronavirus

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd bag_coronavirus
# docker build -t bag_coronavirus --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/StatA/BAG_Coronavirus_Tests":/code/data-processing/bag_coronavirus/data -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/MD-HMW":/code/data-processing/bag_coronavirus/vmdl_data --name bag_coronavirus bag_coronavirus

# Docker on Mac:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing -v ~/PycharmProjects/data-processing/bag_coronavirus/vmdl_data:/code/data-processing/bag_coronavirus/vmdl_data --name bag_coronavirus bag_coronavirus