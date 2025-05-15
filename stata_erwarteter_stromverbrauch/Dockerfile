FROM rocker/rstudio:4.5.0
WORKDIR /code/data-processing/stata_erwarteter_stromverbrauch

RUN apt-get update && apt-get install -y \
    sudo \
    gdebi-core \
    libcairo2-dev \
    libxt-dev \
    libcurl4-openssl-dev libssl-dev \
    r-cran-rstan \
    libxml2-dev \
    default-jdk

RUN R -e "install.packages('renv', repos = c(CRAN = 'https://cloud.r-project.org'))"

COPY stromverbrauch/Productive/renv.lock renv.lock

ENV RENV_PATHS_LIBRARY stromverbrauch/Productive/renv/library

RUN R -e "renv::restore()"

CMD ["Rscript", "/code/data-processing/stata_erwarteter_stromverbrauch/Stromverbrauch_OGD.R"]

# Docker commands to create image and run container:
# cd stata_erwarteter_stromverbrauch
# docker build -t stromverbrauch .
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing -v /mnt/OGD-DataExch/StatA/Stromverbrauch:/code/data-processing/stata_erwarteter_stromverbrauch/data/export --name stromverbrauch stromverbrauch
# On Mac run:
# docker run -it --rm -v /PycharmProjects/data-processing:/code/data-processing --name stromverbrauch stromverbrauch
