# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/awa_feiertage:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/awa_feiertage/data:/code/data -v /mnt/OGD-DataExch/StatA/FreiUndFeiertage:/code/data_orig -v /data/dev/workspace/data-processing/awa_feiertage/change_tracking:/code/change_tracking --name awa_feiertage ghcr.io/opendatabs/data-processing/awa_feiertage:latest
