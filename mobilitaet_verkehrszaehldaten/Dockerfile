FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/mobilitaet_verkehrszaehldaten
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "src.etl"]

# Docker commands to create image and run container locally:
# cd mobilitaet_verkehrszaehldaten
# docker build -t mobilitaet_verkehrszaehldaten .
# docker run -it --rm -v /mnt/MOB-StatA:/code/data-processing/mobilitaet_verkehrszaehldaten/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_verkehrszaehldaten mobilitaet_verkehrszaehldaten

# Run the container with the published image:
# docker run -it --rm -v /mnt/MOB-StatA:/code/data-processing/mobilitaet_verkehrszaehldaten/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_verkehrszaehldaten ghcr.io/opendatabs/data-processing/mobilitaet_verkehrszaehldaten:latest
