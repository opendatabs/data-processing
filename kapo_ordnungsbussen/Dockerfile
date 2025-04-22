# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/kapo_ordnungsbussen:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/kapo_ordnungsbussen/data:/code/data -v  /mnt/OGD-DataExch/KaPo/Ordnungsbussen:/code/data_orig -v /data/dev/workspace/data-processing/kapo_ordnungsbussen/change_tracking:/code/change_tracking --name kapo_ordnungsbussen ghcr.io/opendatabs/data-processing/kapo_ordnungsbussen:latest
