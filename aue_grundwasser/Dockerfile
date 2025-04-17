# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/aue_grundwasser:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/aue_grundwasser/data_orig:/code/data_orig -v /data/dev/workspace/data-processing/aue_grundwasser/data:/code/data -v --name aue_grundwasser ghcr.io/opendatabs/data-processing/aue_grundwasser:latest
