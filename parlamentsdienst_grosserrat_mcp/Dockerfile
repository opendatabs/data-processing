# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

# For installing CA certificates
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/share/ca-certificates

# Python 3.13 from uv: the full-text index needs SQLite 3.43+, the base image's Python has 3.40.
COPY --from=ghcr.io/astral-sh/uv:0.13.9 /uv /uvx /usr/local/bin/
ENV UV_PYTHON_PREFERENCE=only-managed \
    UV_PYTHON=3.13 \
    HF_HOME=/code/.cache/huggingface

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

# The embedding model and its tokenizer, so the job needs no network for them.
RUN uv run python -c "\
from transformers import AutoTokenizer; \
from corpus_mcp.embeddings import MODEL, OnnxEmbedder; \
OnnxEmbedder().passages(['Basel']); AutoTokenizer.from_pretrained(MODEL)"
ENV HF_HUB_OFFLINE=1

CMD ["uv", "run", "-m", "etl"]
