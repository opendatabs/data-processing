# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

RUN apt-get update && apt-get install -y firefox-esr \
    && rm -rf /var/lib/apt/lists/*

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/gva_metadata:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/gva_metadata/data:/code/data --name gva_metadata ghcr.io/opendatabs/data-processing/gva_metadata:latest