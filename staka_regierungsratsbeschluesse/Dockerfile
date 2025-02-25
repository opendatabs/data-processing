FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /code/data-processing
# Copy uv files and install dependencies
COPY staka_regierungsratsbeschluesse/pyproject.toml ./staka_regierungsratsbeschluesse/
COPY staka_regierungsratsbeschluesse/uv.lock ./staka_regierungsratsbeschluesse/
RUN cd staka_regierungsratsbeschluesse && uv sync --frozen¨
COPY . .

CMD ["uv", "run", "-m", "staka_regierungsratsbeschluesse.etl"]

# Docker commands to create image and run container:
# cd staka_regierungsratsbeschluesse
# docker build -t staka_regierungsratsbeschluesse .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name staka_regierungsratsbeschluesse staka_regierungsratsbeschluesse
