# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/gd_coronavirus_abwassermonitoring:latest
# docker run -it --rm -v /mnt/OGD-DataExch/GD-Kantonslabor/Covid-19_Abwasser:/code/data -v /data/dev/workspace/data-processing/gd_coronavirus_abwassermonitoring/change_tracking:/code/change_tracking --name gd_coronavirus_abwassermonitoring ghcr.io/opendatabs/data-processing/gd_coronavirus_abwassermonitoring:latest
