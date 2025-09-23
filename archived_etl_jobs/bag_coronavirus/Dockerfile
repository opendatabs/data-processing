FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.2.2
RUN python3 -m pip install --user --no-cache-dir requests==2.27.1
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir pandasql==0.7.3
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.9
CMD ["/bin/bash", "/code/data-processing/bag_coronavirus/etl_bag_datasets.sh"]
