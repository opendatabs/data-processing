# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ods_harvest:latest
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/FST-OGD:/code/data /data/dev/workspace/data-processing/ods_catalog/change_tracking:/code/change_tracking --name ods_catalog ghcr.io/opendatabs/data-processing/ods_catalog:latest
