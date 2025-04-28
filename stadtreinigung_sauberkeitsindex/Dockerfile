# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stadtreinigung_sauberkeitsindex:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/stadtreinigung_sauberkeitsindex/data:/code/data -v /data/dev/workspace/data-processing/stadtreinigung_sauberkeitsindex/data_agg:/code/data_agg -v /data/dev/workspace/data-processing/stadtreinigung_sauberkeitsindex/change_tracking:/code/change_tracking --name stadtreinigung_sauberkeitsindex ghcr.io/opendatabs/data-processing/stadtreinigung_sauberkeitsindex:latest
