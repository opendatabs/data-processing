# This Dockerfile is planned to be the base image for all data-processing etl jobs.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm

# Set the working directory
WORKDIR /code
 
#### SET THE TIMEZONE TO "Europe/Zurich" ####
# Set environment variables for timezone and locale
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
ENV LANG=de_CH.UTF-8
ENV LANGUAGE=de_CH:de
ENV LC_ALL=de_CH.UTF-8

# Install required system packages 
# Need to run apt-get update first. Otherwise, the installation of locales will fail.
# Need to run rm -rf /var/lib/apt/lists/* to clean up the package cache and reduce the image size.
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
