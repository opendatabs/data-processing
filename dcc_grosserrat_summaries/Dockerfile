# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

# For installing CA certificates
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/share/ca-certificates

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

# The tokenizer that measures the chunks, so the job needs no network for it.
ENV HF_HOME=/code/.cache/huggingface
RUN uv run python -c "from transformers import AutoTokenizer; from corpus_extract.chunking import TOKENIZER; AutoTokenizer.from_pretrained(TOKENIZER)"
ENV HF_HUB_OFFLINE=1

CMD ["uv", "run", "-m", "etl"]
