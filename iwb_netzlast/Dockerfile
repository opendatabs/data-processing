# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/iwb_netzlast:latest
# docker run -it --rm -v /mnt/OGD-DataExch/IWB/Netzlast:/code/data -v /data/dev/workspace/data-processing/iwb_netzlast/change_tracking:/code/change_tracking --name iwb_netzlast ghcr.io/opendatabs/data-processing/iwb_netzlast:latest
