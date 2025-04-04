FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir python-dotenv==1.1.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir geopandas==0.14.3
RUN python3 -m pip install --user --no-cache-dir pygeos==0.14
RUN python3 -m pip install --user --no-cache-dir charset_normalizer==3.3.2
CMD ["python3", "-m", "stadtreinigung_sauberkeitsindex.etl"]


# Docker commands to create image and run container:
# cd stadtreinigung_sauberkeitsindex
# docker build -t stadtreinigung_sauberkeitsindex .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing  --name stadtreinigung_sauberkeitsindex stadtreinigung_sauberkeitsindex

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd stadtreinigung_sauberkeitsindex
# docker build -t stadtreinigung_sauberkeitsindex --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name stadtreinigung_sauberkeitsindex stadtreinigung_sauberkeitsindex
