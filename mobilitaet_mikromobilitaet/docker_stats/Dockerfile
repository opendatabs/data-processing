FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /code/data-processing/mobilitaet_mikromobilitaet
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache
COPY . .

# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

CMD ["uv", "run", "-m", "src.etl_stats"]


# Docker commands to create image and run container:
# cd mobilitaet_mikromobilitaet
# docker build -t mobilitaet_mikromobilitaet_stats .
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name mobilitaet_mikromobilitaet_stats mobilitaet_mikromobilitaet_stats uv run -m src.etl_stats
