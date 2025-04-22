# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/iwb_gas:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/iwb_gas/data:/code/data -v /data/dev/workspace/data-processing/iwb_gas/change_tracking:/code/change_tracking --name iwb_gas ghcr.io/opendatabs/data-processing/iwb_gas:latest
