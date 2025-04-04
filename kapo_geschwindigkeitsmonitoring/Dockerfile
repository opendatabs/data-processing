FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /code/data-processing/kapo_geschwindigkeitsmonitoring
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "src.etl"]


# Run the container with the published image:
# docker run -it --rm -v /mnt/OGD-DataExch/KaPo/VP-Geschwindigkeitsmonitoring:/code/data-processing/kapo_geschwindigkeitsmonitoring/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name kapo_geschwindigkeitsmonitoring ghcr.io/opendatabs/data-processing/kapo_geschwindigkeitsmonitoring:latest
