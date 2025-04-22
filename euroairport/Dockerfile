# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/euroairport:latest
# docker run -it --rm -v /mnt/OGD-DataExch/EuroAirport:/code/data_orig -v /data/dev/workspace/data-processing/euroairport/data:/code/data -v /data/dev/workspace/data-processing/euroairport/change_tracking:/code/change_tracking --name euroairport ghcr.io/opendatabs/data-processing/euroairport:latest
