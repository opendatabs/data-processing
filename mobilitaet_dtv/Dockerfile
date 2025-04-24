# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/mobilitaet_dtv:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/kapo_geschwindigkeitsmonitoring/data:/code/data_orig /data/dev/workspace/data-processing/mobilitaet_dtv/data:/code/data -v /data/dev/workspace/data-processing/mobilitaet_dtv/change_tracking:/code/change_tracking --name mobilitaet_dtv ghcr.io/opendatabs/data-processing/mobilitaet_dtv:latest
