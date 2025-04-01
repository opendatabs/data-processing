FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/stata_harvester
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache

# PYTHONPATH needs to be the parent directory
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "etl"]

# Docker commands to create image and run container:
# cd stata_harvester
# docker build -t stata_harvester .
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name stata_harvester stata_harvester
