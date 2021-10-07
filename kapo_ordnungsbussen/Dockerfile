FROM python:3.9.2
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.25.1
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.7
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir pytest==6.2.4
CMD ["/bin/bash", "/code/data-processing/kapo_ordnungsbussen/etl.sh"]


# Docker commands to create image and run container:
# cd kapo_ordnungsbussen
# docker build -t kapo_ordnungsbussen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/KaPo/Ordnungsbussen:/code/data-processing/kapo_ordnungsbussen/data_orig --name kapo_ordnungsbussen kapo_ordnungsbussen

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd kapo_ordnungsbussen
# docker build -t kapo_ordnungsbussen --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing  -v "//bs.ch/dfs/BS/PD/PD-StatA-FST-OGD-DataExch/KaPo/Ordnungsbussen":/code/data-processing/kapo_ordnungsbussen/data_orig --name kapo_ordnungsbussen kapo_ordnungsbussen
