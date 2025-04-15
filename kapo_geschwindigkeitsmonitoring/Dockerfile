# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/kapo_geschwindigkeitsmonitoring:latest
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data_orig -v /data/dev/workspace/data-processing/kapo_geschwindigkeitsmonitoring/data:/code/data -v /data/dev/workspace/data-processing/common/change_tracking:/code/common/change_tracking --name kapo_geschwindigkeitsmonitoring ghcr.io/opendatabs/data-processing/kapo_geschwindigkeitsmonitoring:latest
