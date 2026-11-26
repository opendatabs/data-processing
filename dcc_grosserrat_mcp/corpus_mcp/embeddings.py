"""Vectors for passages from granite-embedding-311m-multilingual-r2, on the CPU through onnxruntime.

It pools the first token and normalises, as the MCP server does for a query.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path

import numpy as np
import onnxruntime as ort
from huggingface_hub import hf_hub_download
from tokenizers import Tokenizer

MODEL = "ibm-granite/granite-embedding-311m-multilingual-r2"
DIMENSIONS = 768
# Token limit per text: passage plus title and headings.
MAX_TOKENS = 1024


class OnnxEmbedder:
    """The model on the CPU, through onnxruntime."""

    def __init__(self, model: str = MODEL, *, threads: int = 0, batch: int = 32) -> None:
        self.model = model
        self.batch = batch
        self._threads = threads

    def passages(self, texts: Sequence[str]) -> list[bytes]:
        """The vectors of passages, as float32 bytes in the order given."""
        out: list[bytes] = []
        for start in range(0, len(texts), self.batch):
            out.extend(self._encode(list(texts[start : start + self.batch])))
        return out

    @cached_property
    def _tokenizer(self) -> Tokenizer:
        """The model's tokenizer, padding and truncating as the model expects."""
        tokenizer = Tokenizer.from_file(hf_hub_download(self.model, "tokenizer.json"))
        pad = pad_id(self.model)
        tokenizer.enable_padding(pad_id=pad, pad_token=tokenizer.id_to_token(pad))
        tokenizer.enable_truncation(max_length=MAX_TOKENS)
        return tokenizer

    @cached_property
    def _session(self) -> ort.InferenceSession:
        """The ONNX export of the model, on the CPU."""
        options = ort.SessionOptions()
        if self._threads:
            options.intra_op_num_threads = self._threads
        return ort.InferenceSession(
            hf_hub_download(self.model, "onnx/model.onnx"), options, providers=["CPUExecutionProvider"]
        )

    def _encode(self, texts: list[str]) -> list[bytes]:
        encoded = self._tokenizer.encode_batch([text or " " for text in texts])
        ids = np.array([e.ids for e in encoded], dtype=np.int64)
        mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        hidden = np.asarray(self._session.run(None, {"input_ids": ids, "attention_mask": mask})[0])
        return [pack(row) for row in normalise(hidden[:, 0])]


def pad_id(model: str) -> int:
    """The token the model pads with, from its own config; each granite size has another."""
    return int(json.loads(Path(hf_hub_download(model, "config.json")).read_text())["pad_token_id"])


def normalise(vectors: np.ndarray) -> np.ndarray:
    """Scale rows to length one, so the dot product is the cosine."""
    lengths = np.linalg.norm(vectors, axis=-1, keepdims=True)
    return vectors / np.maximum(lengths, 1e-12)


def pack(vector: Sequence[float]) -> bytes:
    """A vector as the float32 bytes sqlite-vec stores."""
    return struct.pack(f"{len(vector)}f", *vector)
