# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/tba_abfuhrtermine:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/tba_abfuhrtermine/data:/code/data -v /mnt/OGD-GVA:/code/data_orig -v /data/dev/workspace/data-processing/tba_abfuhrtermine/change_tracking:/code/change_tracking --name tba_abfuhrtermine ghcr.io/opendatabs/data-processing/tba_abfuhrtermine:latest
