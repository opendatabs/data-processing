# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ed_schulferien:latest
# docker run -it --rm -v /mnt/OGD-DataExch/ED-Schulferien:/code/data_orig -v /data/dev/workspace/data-processing/ed_schulferien/data:/code/data -v /data/dev/workspace/data-processing/ed_schulferien/change_tracking:/code/change_tracking --name ed_schulferien ghcr.io/opendatabs/data-processing/ed_schulferien:latest
