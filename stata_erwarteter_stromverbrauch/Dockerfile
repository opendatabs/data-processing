# install the Prophet R package inside of Docker container:
# https://github.com/alexeybutyrev/dockerprophet

FROM rocker/rstudio:4.2.1
#FROM rocker/r-base
## Using a base image with R4.2.1 and RSTUDIO_VERSION=2022.07.2+576
WORKDIR /code/data-processing

#RUN apt-get update && apt-get install -y \
#    sudo=1.8.31-1ubuntu1.4 \
#    gdebi-core=0.9.5.7+nmu3 \
#    libcairo2-dev=1.16.0-4ubuntu1 \
#    libxt-dev=1:1.1.5-1 \
#    libcurl4=7.68.0-1ubuntu2 \
#    libcurl4-openssl-dev=7.68.0-1ubuntu2  \
#    libssl-dev=1.1.1f-1ubuntu2.17 \
#    r-cran-rstan=2.19.2-1build1



RUN apt-get update && apt-get install -y \
    sudo \
    gdebi-core \
    libcairo2-dev \
    libxt-dev \
    libcurl4-openssl-dev libssl-dev \
    r-cran-rstan


## Explicitly setting my default RStudio Package Manager Repo
## Uses packages as at 30/06/2022
RUN echo "r <- getOption('repos'); \
	  r['CRAN'] <- 'https://packagemanager.rstudio.com/cran/__linux__/focal/2022-06-30'; \
	  options(repos = r);" > ~/.Rprofile



RUN install2.r \
    --skipinstalled \
    httr \
    data.table \
    dplyr \
    tidyr \
    lubridate \
    ggplot2 \
    stringr \
    fastDummies \
    zoo \
    forecast \
    -e prophet


CMD ["Rscript", "/code/data-processing/stata_erwarteter_stromverbrauch/Stromverbrauch_OGD.R"]

# Docker commands to create image and run container:
# cd stata_erwarteter_stromverbrauch
# docker build -t stromverbrauch .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/StatA/Stromverbrauch:/code/data-processing/stata_erwarteter_stromverbrauch/data/export --name stromverbrauch stromverbrauch
