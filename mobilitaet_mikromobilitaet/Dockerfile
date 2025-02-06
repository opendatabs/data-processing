FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY

# Set environment variables for timezone and locale
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
ENV LANG=de_CH.UTF-8
ENV LANGUAGE=de_CH:de
ENV LC_ALL=de_CH.UTF-8

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    locales \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Configure the system locale
RUN echo "de_CH.UTF-8 UTF-8" >> /etc/locale.gen && \
    locale-gen de_CH.UTF-8 && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=de_CH.UTF-8

# Set the timezone
RUN ln -fs /usr/share/zoneinfo/Europe/Zurich /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata


RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir geopandas==1.0.1
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir charset_normalizer==3.3.2
RUN python3 -m pip install --user --no-cache-dir owslib==0.31.0
CMD ["python3", "-m", "mobilitaet_mikromobilitaet.etl"]


# Docker commands to create image and run container:
# cd mobilitaet_mikromobilitaet
# docker build -t mobilitaet_mikromobilitaet .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_mikromobilitaet mobilitaet_mikromobilitaet python3 -m mobilitaet_mikromobilitaet.etl
