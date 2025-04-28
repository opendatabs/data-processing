# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/staka_kandidaturen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/staka_kandidaturen/data:/code/data -v /data/dev/workspace/data-processing/staka_kandidaturen/change_tracking:/code/change_tracking -v /mnt/OGD-DataExch/StatA/Wahlen-Abstimmungen/Kandidaturen:/code/data_orig --name staka_kandidaturen ghcr.io/opendatabs/data-processing/staka_kandidaturen:latest
