import json
import logging
import os
from pathlib import Path

import common
import joblib
import truststore

truststore.inject_into_ssl()

EMBED_URL = os.environ["QWEN_EMBED_URL"].rstrip("/")
EMBED_MODEL = os.environ["QWEN_EMBED_MODEL"]
EMBED_API_KEY = os.getenv("QWEN_EMBED_API_KEY")
EMBED_HEADERS = {"Authorization": f"Bearer {EMBED_API_KEY}"} if EMBED_API_KEY else {}

MARKDOWN_DIR = Path("data_orig/markdown/gr_dokumente_md_docling-serve")
MODEL_DIR = Path("data/model")
OUTPUT_DIR = Path("data")


def main():
    card = json.loads((MODEL_DIR / "karte.json").read_text(encoding="utf-8"))
    estimator = joblib.load(MODEL_DIR / "modell.joblib")
    logging.info(f"Model in {MODEL_DIR}: trained {card['trainiert_am']}, {len(card['themen'])} topics")

    models = common.requests_get(f"{EMBED_URL}/models", headers=EMBED_HEADERS, timeout=60).json()["data"]
    offered = {m["id"]: m.get("root") for m in models}
    if EMBED_MODEL not in offered:
        raise RuntimeError(f"{EMBED_URL} does not offer {EMBED_MODEL}, only {list(offered)}")
    if card["embed_model"] not in (EMBED_MODEL, offered[EMBED_MODEL]):
        raise RuntimeError(f"{EMBED_MODEL} runs {offered[EMBED_MODEL]}, the model was trained on {card['embed_model']}")
    r = common.requests_post(
        f"{EMBED_URL}/embeddings",
        json={"model": EMBED_MODEL, "input": ["Grosser Rat"]},
        headers=EMBED_HEADERS,
        timeout=60,
    )
    dimensions = len(r.json()["data"][0]["embedding"])
    if dimensions != estimator.n_features_in_:
        raise RuntimeError(
            f"{EMBED_MODEL} returns {dimensions} dimensions, the model expects {estimator.n_features_in_}"
        )
    logging.info(f"{EMBED_URL} serves {EMBED_MODEL} with {dimensions} dimensions")

    if not MARKDOWN_DIR.is_dir():
        raise RuntimeError(f"{MARKDOWN_DIR} not found")
    logging.info(f"{MARKDOWN_DIR}: {sum(1 for _ in MARKDOWN_DIR.iterdir())} files")

    probe = OUTPUT_DIR / "write.check"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink()
    logging.info(f"{OUTPUT_DIR} is writable")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main()
    logging.info("Check successful.")
