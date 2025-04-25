# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ods_publish:latest
# docker run -it --rm --name ods_publish ghcr.io/opendatabs/data-processing/ods_publish:latest uv run -m etl_id 100087
# OR
# docker run -it --rm --name ods_publish ghcr.io/opendatabs/data-processing/ods_publish:latest uv run -m etl da_20e9bc,da_uxt6fk,da_q78iuw,da_reclv8


