"""Ask Gemma and get a usable answer back."""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx
from openai import APIStatusError, LengthFinishReasonError, OpenAI, OpenAIError
from pydantic import ValidationError

from corpus_extract.claims import Sweep
from corpus_extract.prompts import CHUNK_NOTE, SYSTEM, USER_TEMPLATE

# Room for the whole schema.
MAX_TOKENS = 6144
TIMEOUT = 240.0
# Greedy decoding can loop on one phrase until the limit; the second try penalises repeated tokens.
PENALTIES = (0.0, 0.3)
TRUNCATED = "answer truncated"


class SweepError(RuntimeError):
    """The model could not give a usable answer for this piece."""


@dataclass(frozen=True, slots=True)
class Request:
    """A piece of a document as it is put to the model."""

    body: str
    titel_dok: str = ""
    titel_ges: str = ""
    signatur: str = ""
    chunk: int = 1
    chunks: int = 1
    pages: str = ""

    def messages(self) -> list[dict[str, str]]:
        """The two messages that make up the request."""
        note = (
            CHUNK_NOTE.format(chunk=self.chunk, chunks=self.chunks, pages=self.pages or "?") if self.chunks > 1 else ""
        )
        return [
            {"role": "system", "content": SYSTEM},
            {
                "role": "user",
                "content": USER_TEMPLATE.format(
                    titel_dok=self.titel_dok or "unknown",
                    titel_ges=self.titel_ges or "unknown",
                    signatur=self.signatur or "unknown",
                    chunk_note=note,
                    body=self.body,
                ),
            },
        ]


def client(url: str, *, api_key: str = "", http_client: httpx.Client | None = None) -> OpenAI:
    """The client for Gemma; the SDK requires a key even where the server needs none."""
    # The SDK annotates its bundled httpx; any httpx client works at runtime.
    return OpenAI(
        base_url=url,
        api_key=api_key or "none",
        timeout=TIMEOUT,
        max_retries=2,
        http_client=http_client,  # ty: ignore[invalid-argument-type]
    )


def check(llm: OpenAI, model: str) -> None:
    """Fail unless the server answers and serves the model."""
    try:
        served = [entry.id for entry in llm.models.list().data]
    except APIStatusError as error:
        answer = " ".join(error.response.text.split())[:300]
        server = error.response.headers.get("server", "unknown")
        raise RuntimeError(
            f"{llm.base_url} refused with {error.status_code} ({answer}; server: {server}): "
            "check GEMMA_API_KEY, GEMMA_URL and no_proxy"
        ) from error
    except OpenAIError as error:
        raise RuntimeError(f"{llm.base_url} does not answer ({error}): check GEMMA_URL and no_proxy") from error
    if model not in served:
        raise RuntimeError(f"{llm.base_url} serves {served or ['nothing']}, not {model}: check GEMMA_MODEL")


def sweep(llm: OpenAI, request: Request, *, model: str, looped: bool = False) -> Sweep:
    """Let the model read a piece and return what it found in it; a piece that looped before starts penalised."""
    for penalty in PENALTIES[1:] if looped else PENALTIES:
        try:
            answer = llm.chat.completions.parse(
                model=model,
                temperature=0.0,
                frequency_penalty=penalty,
                max_tokens=MAX_TOKENS,
                messages=request.messages(),  # ty: ignore[invalid-argument-type]
                response_format=Sweep,
            )
        except LengthFinishReasonError:
            continue
        except OpenAIError as error:
            raise SweepError(f"request failed: {error}") from error
        except ValidationError as error:
            raise SweepError(f"answer did not match the schema: {error}") from error
        if answer.choices[0].message.parsed is None:
            raise SweepError("the model returned nothing")
        return answer.choices[0].message.parsed
    raise SweepError(f"{TRUNCATED} at {MAX_TOKENS} tokens")


def as_json(sweep_: Sweep) -> str:
    """Write the sweep the way it is stored in the table."""
    return json.dumps(sweep_.model_dump(mode="json"), ensure_ascii=False)
