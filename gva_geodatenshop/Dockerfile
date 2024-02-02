FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
CMD ["python3", "-m", "gva_geodatenshop.etl"]


# Docker commands to create image and run container:
# cd gva_geodatenshop
# docker build -t gva-geodatenshop .
# cd ..
# docker run -it --rm -v /mnt/OGD-GVA:/code/data-processing/gva_geodatenshop/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name gva-geodatenshop gva-geodatenshop

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd gva_geodatenshop
# docker build -t gva-geodatenshop --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-Data-GVA":/code/data-processing/gva_geodatenshop/data_orig -v "C:\dev\workspace\data-processing":/code/data-processing --name gva-geodatenshop gva-geodatenshop
