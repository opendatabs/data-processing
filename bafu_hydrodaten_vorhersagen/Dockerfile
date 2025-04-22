# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/bafu_hydrodaten_vorhersagen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/bafu_hydrodaten_vorhersagen/data:/code/data -v /data/dev/workspace/data-processing/bafu_hydrodaten_vorhersagen/change_tracking:/code/change_tracking --name bafu_hydrodaten_vorhersagen ghcr.io/opendatabs/data-processing/bafu_hydrodaten_vorhersagen:latest
