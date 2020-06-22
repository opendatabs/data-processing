FROM python:3.7.6
WORKDIR /code
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --requirement requirements.txt
CMD ["/bin/bash", "/code/data-processing/aue_schall/etl.sh"]


# Docker commands to create image and run container:
# cd aue_schall
# docker build -t aue_schall .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing  --name aue_schall aue_schall

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd aue_schall
# docker build -t aue_schall --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name aue_schall aue_schall
