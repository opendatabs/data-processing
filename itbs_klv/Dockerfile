# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/itbs_klv:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/itbs_klv/data:/code/data -v /data/dev/workspace/data-processing/itbs_klv/data_orig:/code/data_orig -v /data/dev/workspace/data-processing/itbs_klv/change_tracking:/code/change_tracking --name itbs_klv ghcr.io/opendatabs/data-processing/itbs_klv:latest
