FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/mobilitaet_mikromobilitaet_stats
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache


# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "etl"]


# Run the container with the published image:
# docker run -it --rm -v /mnt/OGD-DataExch/StatA/BVD-MOB/Mikromobilitaet:/code/data-processing/mobilitaet_mikromobilitaet_stats/data -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_mikromobilitaet_stats ghcr.io/opendatabs/data-processing/mobilitaet_mikromobilitaet_stats:latest
