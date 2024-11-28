FROM rocker/rstudio:4.2.1
#FROM rocker/r-base
## Using a base image with R4.2.1 and RSTUDIO_VERSION=2022.07.2+576
WORKDIR /code/data-processing/stata_konoer

RUN apt-get update && apt-get install -y \
    sudo \
    gdebi-core \
    libcairo2-dev \
    libxt-dev \
    libcurl4 \
    libcurl4-openssl-dev \
    libssl-dev \
    r-cran-rstan \
    libxml2-dev \
    default-jdk \
    libglpk-dev \
    libudunits2-dev \
    libproj-dev \
    libgdal-dev


## Explicitly setting my default RStudio Package Manager Repo
## Uses packages as at 28/11/2024
RUN echo "r <- getOption('repos'); \
	  r['CRAN'] <- 'https://packagemanager.rstudio.com/cran/__linux__/focal/2024-11-28'; \
	  options(repos = r);" > ~/.Rprofile

RUN Rscript -e "install.packages(c('zoo', 'data.table', 'lubridate', 'knitr', 'tidyverse', 'eRTG3D'), dependencies = TRUE)"

CMD ["Rscript", "/code/data-processing/stata_konoer/etl.R"]

# Docker commands to create image and run container:
# cd stata_konoer
# docker build -t stata_konoer .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/StatA/KoNör:/code/data-processing/stata_konoer/data --name stata_konoer stata_konoer
