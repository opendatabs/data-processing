FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
CMD ["python3", "-m", "gd_coronavirus_abwassermonitoring.make_dataset"]


# Docker commands to create image and run container:
# cd gd_coronavirus_abwassermonitoring
# docker build -t gd_coronavirus_abwassermonitoring .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/GD-Kantonslabor/Covid-19_Abwasser:/code/data-processing/gd_coronavirus_abwassermonitoring/data --name gd_coronavirus_abwassermonitoring gd_coronavirus_abwassermonitoring

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd gd_coronavirus_abwassermonitoring
# docker build -t gd_coronavirus_abwassermonitoring --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/GD-Kantonslabor/Covid19_Abwasser":/code/data-processing/gd_coronavirus_abwassermonitoring/data  --name gd_coronavirus_abwassermonitoring gd_coronavirus_abwassermonitoring


