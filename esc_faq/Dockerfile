# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/esc_faq:latest
# docker run -it --rm -v /mnt/OGD-DataExch/PD-ESC-FAQ/FAQs:/code/data_orig -v /data/dev/workspace/data-processing/esc_faq/data:/code/data -v /data/dev/workspace/data-processing/esc_faq/change_tracking:/code/change_tracking --name esc_faq ghcr.io/opendatabs/data-processing/esc_faq:latest
