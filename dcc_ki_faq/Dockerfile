# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/dcc_ki_faq:latest
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/KI-FAQ:/code/data_orig -v /data/dev/workspace/data-processing/dcc_ki_faq/data:/code/data -v /data/dev/workspace/data-processing/dcc_ki_faq/change_tracking:/code/change_tracking --name dcc_ki_faq ghcr.io/opendatabs/data-processing/dcc_ki_faq:latest
