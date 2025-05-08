# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/parlamentsdienst_grosserrat_datasette:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/parlamentsdienst_grosserrat_datasette/data:/code/data -v /data/dev/workspace/data-processing/parlamentsdienst_grosserrat/data/export:/code/data_orig --name parlamentsdienst_grosserrat ghcr.io/opendatabs/data-processing/parlamentsdienst_gr_abstimmungen:latest
