# install the Prophet R package inside of Docker container:
# https://github.com/alexeybutyrev/dockerprophet

FROM rocker/rstudio:4.1.2
#FROM rocker/r-base
## Using a base image with R4.1.2 and RStudio version 2021.09.1+372
WORKDIR /Users/hester/RstudioProjects/stromverbrauch

## Check for updates
RUN apt-get update && apt-get install -y \
    sudo \
    gdebi-core \
    libcairo2-dev \
    libxt-dev \
    libcurl4-openssl-dev libssl-dev \
    r-cran-rstan


## Explicitly setting my default RStudio Package Manager Repo
## Uses packages as at 24/12/2022
RUN echo "r <- getOption('repos'); \
	  r['CRAN'] <- 'https://packagemanager.rstudio.com/cran/__linux__/focal/2021-12-24'; \
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


CMD ["Rscript", "Stromverbrauch_OGD_server.R"]

# Docker commands to create image and run container:
# cd stromverbrauch
# docker build -t stromverbrauch .
# cd ..
# docker run -it --rm -v  /Users/hester/RstudioProjects/stromverbrauch:/Users/hester/RstudioProjects/stromverbrauch --name stromverbrauch stromverbrauch
