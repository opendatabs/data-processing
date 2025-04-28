# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stata_parzellen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stata_parzellen/data:/code/data -v -v /mnt/OGD-DataExch/StatA/Parzellen:/code/data_orig -v /data/dev/workspace/data-processing/stata_parzellen/change_tracking:/code/change_tracking --name stata_parzellen ghcr.io/opendatabs/data-processing/stata_parzellen:latest
