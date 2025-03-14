FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /code/data-processing/meteoblue_rosental
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache
COPY . .

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv run", "-m", "etl"]


# Docker commands to create image and run container:
# cd meteoblue_rosental
# docker build -t meteoblue_rosental .
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name meteoblue_rosental meteoblue_rosental uv run -m etl
