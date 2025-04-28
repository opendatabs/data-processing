# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/staka_abstimmungen:latest
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/Wahlen-Abstimmungen:/code/data -v /data/dev/workspace/data-processing/staka_abstimmungen/change_tracking:/code/change_tracking --name staka_abstimmungen ghcr.io/opendatabs/data-processing/staka_abstimmungen:latest
