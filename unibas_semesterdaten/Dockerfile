# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]



# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/unibas_semesterdaten:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/unibas_semesterdaten/data:/code/data -v /data/dev/workspace/data-processing/unibas_semesterdaten/change_tracking:/code/change_tracking  --name unibas_semesterdaten ghcr.io/opendatabs/data-processing/unibas_semesterdaten:latest