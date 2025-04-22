# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ibs_parkhaus_bewegungen:latest
# docker run -it --rm -v /mnt/OGD-DataExch/ibs-parkhaeuser/Ein-Ausfahrten:/code/data_orig -v /data/dev/workspace/data-processing/ibs_parkhaus_bewegungen/data:/code/data -v /data/dev/workspace/data-processing/ibs_parkhaus_bewegungen/change_tracking:/code/change_tracking --name ibs_parkhaus_bewegungen ghcr.io/opendatabs/data-processing/ibs_parkhaus_bewegungen:latest
