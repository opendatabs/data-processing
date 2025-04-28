# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stata_bik:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stata_bik/data:/code/data -v /data/dev/workspace/data-processing/stata_bik/data_orig:/code/data_orig -v /data/dev/workspace/data-processing/stata_bik/change_tracking:/code/change_tracking --name stata_bik ghcr.io/opendatabs/data-processing/stata_bik:latest
