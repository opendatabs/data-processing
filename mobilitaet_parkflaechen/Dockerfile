FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir python-dotenv==1.1.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir geopandas==1.0.1
RUN python3 -m pip install --user --no-cache-dir pygeos==0.14
CMD ["python3", "-m", "mobilitaet_parkflaechen.etl"]


# Docker commands to create image and run container:
# cd mobilitaet_parkflaechen
# docker build -t mobilitaet_parkflaechen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/BVD-mobilitaet/Parkplatzkataster/Shape:/code/data-processing/mobilitaet_parkflaechen/data_orig  --name mobilitaet_parkflaechen mobilitaet_parkflaechen