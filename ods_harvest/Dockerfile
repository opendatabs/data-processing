# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ods_harvest:latest
# docker run -it --rm --name ods_harvest ghcr.io/opendatabs/data-processing/ods_harvest:latest uv run -m etl gva-gpkg-ftp-csv
