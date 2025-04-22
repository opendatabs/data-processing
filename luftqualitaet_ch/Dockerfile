# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/luftqualitaet_ch:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/luftqualitaet_ch/data:/code/data -v /data/dev/workspace/data-processing/luftqualitaet_ch/change_tracking:/code/change_tracking --name luftqualitaet_ch ghcr.io/opendatabs/data-processing/luftqualitaet_ch:latest
