FROM python:3.10.0
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir pandasql==0.7.3
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir MechanicalSoup==1.1.0
CMD ["/bin/bash", "/code/data-processing/gsv_covid19_hosp/etl.sh"]


# Docker commands to create image and run container:
# cd gsv_covid19_hosp
# docker build -t gsv_covid19_hosp .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name gsv_covid19_hosp gsv_covid19_hosp

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd gsv_covid19_hosp
# docker build -t gsv_covid19_hosp --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  --name gsv_covid19_hosp gsv_covid19_hosp
