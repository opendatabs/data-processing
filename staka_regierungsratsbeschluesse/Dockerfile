FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/staka_regierungsratsbeschluesse
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache
COPY . .

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "etl"]

# Docker commands to create image and run container:
# cd staka_regierungsratsbeschluesse
# docker build -t staka_regierungsratsbeschluesse .
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name staka_regierungsratsbeschluesse staka_regierungsratsbeschluesse
