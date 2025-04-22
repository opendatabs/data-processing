# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "src.etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/bvb_fahrgastzahlen:latest
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/BVB/Fahrgastzahlen:/code/data_orig -v /data/dev/workspace/data-processing/bvb_fahrgastzahlen/data:/code/data -v /data/dev/workspace/data-processing/bvb_fahrgastzahlen/change_tracking:/code/change_tracking --name bvb_fahrgastzahlen ghcr.io/opendatabs/data-processing/bvb_fahrgastzahlen:latest
