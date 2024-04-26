FROM python:3.11-bullseye
WORKDIR /code/data-processing

# Installieren von locale-gen und dem deutschen Locale um die Monate auf Deutsch zu lesen
RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/* \
    && sed -i -e 's/# de_DE.UTF-8 UTF-8/de_DE.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen

# Setzen des Locale
ENV LANG de_DE.UTF-8  
ENV LANGUAGE de_DE:de  
ENV LC_ALL de_DE.UTF-8

#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
CMD ["python3", "-m", "bvb_fahrgastzahlen.etl"]


# Docker commands to create image and run container:
# cd bvb_fahrgastzahlen
# docker build -t bvb_fahrgastzahlen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name bvb_fahrgastzahlen bvb_fahrgastzahlen
