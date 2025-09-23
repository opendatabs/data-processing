FROM rocker/r-base
WORKDIR /usr/src/meteoblue_wolf
COPY install_packages.R .
COPY Fieldclimate2JSON.R .

RUN apt-get update -qq && apt-get -y --no-install-recommends install libcurl4-openssl-dev curl
RUN Rscript ./install_packages.R
CMD Rscript ./Fieldclimate2JSON.R
