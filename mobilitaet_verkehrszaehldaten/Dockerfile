# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/mobilitaet_verkehrszaehldaten:latest
# docker run -it --rm -v /mnt/MOB-StatA:/code/data_orig -v /data/dev/workspace/data-processing/mobilitaet_verkehrszaehldaten/data:/code/data -v /data/dev/workspace/data-processing/mobilitaet_verkehrszaehldaten/change_tracking:/code/change_tracking --name mobilitaet_verkehrszaehldaten ghcr.io/opendatabs/data-processing/mobilitaet_verkehrszaehldaten:latest
