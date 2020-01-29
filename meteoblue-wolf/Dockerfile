FROM rocker/r-base
WORKDIR /usr/src/meteoblue
COPY install_packages.R .
COPY Fieldclimate2JSON.R .
COPY credentials.R .
# COPY . .
RUN apt-get update -qq && apt-get -y --no-install-recommends install libcurl4-openssl-dev curl
RUN Rscript ./install_packages.R
CMD Rscript ./Fieldclimate2JSON.R

# docker build -t meteoblue_fieldclimate2csv .
# docker run -it --rm --mount type=bind,source=$(pwd)/output/csv,target=/usr/src/meteoblue/output/csv --name meteoblue_fieldclimate2csv meteoblue_fieldclimate2csv

# Remove all images and containers from disk, even if running:
# docker system prune -a