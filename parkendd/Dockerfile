FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
COPY data/csv/lots/parkhaeuser_manually_curated.csv parkendd/data/csv/lots/
RUN python3 -m pip install --user --no-cache-dir beautifulsoup4==4.12.3
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir python-dotenv==1.1.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir python-dateutil==2.8.2
CMD ["python3", "-m", "parkendd.etl"]


# Docker commands to create image and run container:
# cd parkendd
# docker build -t parkendd .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name parkendd parkendd

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd parkendd
# docker build -t parkendd --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name parkendd parkendd
