FROM rocker/rstudio:4.3.3
# Base image with R 4.3.3 and RStudio

WORKDIR /code/data-processing/stata_konoer

# Install required system dependencies
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
    libgdal-dev \
    libicu-dev \
    locales && \
    locale-gen de_DE.UTF-8 && \
    update-locale LANG=de_DE.UTF-8

# Set environment variables for German locale
ENV LANG=de_DE.UTF-8
ENV LANGUAGE=de_DE:de
ENV LC_ALL=de_DE.UTF-8

# Set default RStudio Package Manager Repository
RUN echo "r <- getOption('repos'); \
          r['CRAN'] <- 'https://packagemanager.rstudio.com/cran/__linux__/focal/2024-11-28'; \
          options(repos = r);" > ~/.Rprofile

# Install required R packages
RUN Rscript -e "install.packages(c('zoo', 'data.table', 'lubridate', 'knitr', 'tidyverse', 'eRTG3D', 'httr', 'stringi'), dependencies = TRUE)"

# Set the default command to execute the R script
CMD ["Rscript", "/code/data-processing/stata_konoer/etl.R"]

# Docker commands to create image and run container:
# cd stata_konoer
# docker build -t stata_konoer .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing \
#     -v /mnt/OGD-DataExch/StatA/KoNör:/code/data-processing/stata_konoer/data \
#     --name stata_konoer stata_konoer
