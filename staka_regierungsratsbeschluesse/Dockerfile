FROM python:3.12-slim

# Install system dependencies for WeasyPrint and Poppler for pdf2image
RUN apt-get update && apt-get install -y \
    libgdk-pixbuf2.0-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libglib2.0-0 \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/staka_regierungsratsbeschluesse
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache


# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "src.etl"]

# Docker commands to create image and run container:
# cd staka_regierungsratsbeschluesse
# docker build -t staka_regierungsratsbeschluesse .
# docker run -it --rm -e https_proxy="http://your-proxy-server:port" -v /data/dev/workspace/data-processing:/code/data-processing --name staka_regierungsratsbeschluesse staka_regierungsratsbeschluesse uv run -m src.etl
