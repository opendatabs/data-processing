# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/update_temporal_coverage:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stata_ods/daily_jobs/update_temporal_coverage:/code --name ods_catalog ghcr.io/opendatabs/data-processing/ods_catalog:latest
