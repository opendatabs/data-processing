FROM python:3.12-slim-bookworm
# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /code/data-processing
# Copy uv files and install dependencies
COPY pyproject.toml ./
COPY uv.lock ./
RUN uv sync --frozen

CMD ["uv", "run", "-m" "staka_regierungsratsbeschluesse.etl"]