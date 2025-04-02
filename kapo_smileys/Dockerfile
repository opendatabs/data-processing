FROM python:3.12-slim
# Install SpatiaLite dependencies
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         spatialite-bin \
#         libsqlite3-mod-spatialite
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/kapo_smileys
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "src.etl"]

# Run the container with the published image:
# docker run -it --rm -v /mnt/OGD-DataExch/kapo-smileys:/code/data-processing/kapo_smileys/data_orig -v /data/dev/workspace/data-processing:/code/data-processing  --name kapo_smileys ghcr.io/opendatabs/data-processing/kapo_smileys:latest
