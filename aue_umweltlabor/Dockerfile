FROM python:3.7.6
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas
# CMD ["python3", "-m", "aue_umweltlabor.etl"]
CMD ["/bin/bash", "/code/data-processing/aue_umweltlabor/etl.sh"]


# Docker commands to create image and run container:
# cd aue_umweltlabor
# docker build -t aue-umweltlabor .
# docker run -it --rm -v /mnt/OGD-DataExch/Umweltlabor:/code/data-processing/aue_umweltlabor/data_orig -v /home/jonas.bieri/dev/workspace:/code/data-processing --name aue-umweltlabor aue-umweltlabor

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd aue_umweltlabor
# docker build -t aue-umweltlabor --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name aue-umweltlabor aue-umweltlabor


