# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/gva_geodatenshop:latest
# docker run -it --rm -v /mnt/OGD-DataExch/GD-Kantonslabor/Influenza_RSV_Abwasser:/code/data -v /mnt/OGD-DataExch/StatA/harvesters/GVA:/code/data_harvester -v /data/dev/workspace/data-processing/gva_geodatenshop/change_tracking:/code/change_tracking --name gva_geodatenshop ghcr.io/opendatabs/data-processing/gva_geodatenshop:latest
