# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/kapo_eventverkehr_stjakob:latest
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo-Eventverkehr-St.Jakob:/code/data_orig -v /data/dev/workspace/data-processing/kapo_eventverkehr_stjakob/data:/code/data -v /data/dev/workspace/data-processing/kapo_eventverkehr_stjakob/change_tracking:/code/change_tracking --name kapo_eventverkehr_stjakob ghcr.io/opendatabs/data-processing/kapo_eventverkehr_stjakob:latest
