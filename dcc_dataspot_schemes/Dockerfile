# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/dcc_dataspot_schemes:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/dcc_dataspot_schemes/data:/code/data /data/dev/workspace/data-processing/dcc_dataspot_schemes/change_tracking:/code/change_tracking --name dcc_dataspot_schemes ghcr.io/opendatabs/data-processing/dcc_dataspot_schemes:latest
