FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
CMD ["python3", "-m", "stata_parzellen.etl"]


# Docker commands to create image and run container:
# cd stata_parzellen
# docker build -t stata_parzellen .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/Parzellen:/code/data-processing/stata_parzellen/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name stata_parzellen stata_parzellen

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd stata_parzellen
# docker build -t stata_parzellen --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name stata_parzellen stata_parzellen


