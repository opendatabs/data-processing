# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/staka_baupublikationen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/staka_baupublikationen/data:/code/data -v /data/dev/workspace/data-processing/staka_baupublikationen/change_tracking:/code/change_tracking --name staka_baupublikationen ghcr.io/opendatabs/data-processing/staka_baupublikationen:latest
