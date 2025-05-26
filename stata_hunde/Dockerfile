# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stata_hunde:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stata_hunde/change_tracking:/code/change_tracking -v /mnt/OGD-DataExch/StatA/Hunde:/code/data --name stata_hunde ghcr.io/opendatabs/data-processing/stata_hunde:latest
