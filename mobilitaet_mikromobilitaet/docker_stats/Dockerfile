FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir geopandas==1.0.1
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir charset_normalizer==3.3.2
CMD ["python3", "-m", "mobilitaet_mikromobilitaet.src.etl_stats"]


# Docker commands to create image and run container:
# cd mobilitaet_mikromobilitaet
# docker build -t mobilitaet_mikromobilitaet_stats .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_mikromobilitaet_stats mobilitaet_mikromobilitaet_stats python3 -m mobilitaet_mikromobilitaet.src.etl_stats
