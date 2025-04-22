# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl_https"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/bafu_hydrodaten:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/bafu_hydrodaten/data:/code/data -v /data/dev/workspace/data-processing/bafu_hydrodaten/change_tracking:/code/change_tracking --name bafu_hydrodaten ghcr.io/opendatabs/data-processing/bafu_hydrodaten:latest
