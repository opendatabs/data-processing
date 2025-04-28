# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stata_befragungen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stata_befragungen/data:/code/data -v /data/dev/workspace/data-processing/stata_befragungen/data_orig:/code/data_orig -v /data/dev/workspace/data-processing/stata_befragungen/change_tracking:/code/change_tracking -v /mnt/OGD-DataExch/StatA/Befragungen/55plus_Ablage_StatA:/code/data_orig/55plus -v /mnt/OGD-DataExch/StatA/Befragungen/55plus_OGD:/code/data/55plus --name stata_befragungen ghcr.io/opendatabs/data-processing/stata_befragungen:latest
