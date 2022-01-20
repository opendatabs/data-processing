FROM python:3.10.0
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.27.1
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
CMD ["python3", "-m", "smarte_strasse_schall.etl"]

# Docker commands to create image and run container:
# cd smarte_strasse_schall
# docker build -t smarte_strasse_schall .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name smarte_strasse_schall smarte_strasse_schall

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd smarte_strasse_schall
# docker build -t smarte_strasse_schall --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name smarte_strasse_schall smarte_strasse_schall
