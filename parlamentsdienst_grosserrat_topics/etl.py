import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import common
import joblib
import numpy as np
import pandas as pd
import truststore
from sklearn.preprocessing import normalize

truststore.inject_into_ssl()

EMBED_URL = os.environ["QWEN_EMBED_URL"].rstrip("/")
EMBED_MODEL = os.environ["QWEN_EMBED_MODEL"]
EMBED_API_KEY = os.getenv("QWEN_EMBED_API_KEY")
EMBED_HEADERS = {"Authorization": f"Bearer {EMBED_API_KEY}"} if EMBED_API_KEY else {}

MARKDOWN_DIR = Path("data_orig/markdown/gr_dokumente_md_docling-serve")
MODEL_DIR = Path("data/model")
TOPICS_FILE = Path("data/topics.csv")
TOP = 2
COLUMNS = [
    "signatur_ges",
    *(column for k in range(1, TOP + 1) for column in (f"topic_{k}", f"confidence_{k}")),
    "source",
    "model",
    "dok_laufnr",
    "assigned_at",
]
PRIORITY = ("Text ", "Text", "Anzug", "Motion", "Interpellation", "Schriftliche Anfrage")
MAX_CHARS = 8000
EMBED_BATCH = 16
SAVE_EVERY = 256


def get_dataset(ods_id: str, fields: list[str]) -> pd.DataFrame:
    r = common.requests_get(
        f"https://data.bs.ch/explore/dataset/{ods_id}/download/", params={"format": "csv"}, timeout=300
    )
    return pd.read_csv(io.BytesIO(r.content), sep=";", dtype=str, keep_default_na=False, usecols=fields)


def get_labels() -> pd.DataFrame:
    df = get_dataset("100086", ["signatur", "thema_1", "thema_2"]).apply(lambda col: col.str.strip())
    df = df[(df["signatur"] != "") & (df["thema_1"] != "")].drop_duplicates("signatur")
    df = df.rename(columns={"signatur": "signatur_ges", "thema_1": "topic_1", "thema_2": "topic_2"})
    return df.set_index("signatur_ges").assign(source="ogd").reindex(columns=COLUMNS[1:], fill_value="")


def get_documents() -> pd.DataFrame:
    df = get_dataset("100313", ["dok_laufnr", "signatur_ges", "titel_dok"])
    df = df[(df["signatur_ges"] != "") & (df["dok_laufnr"] != "")]
    df["dok_laufnr"] = df["dok_laufnr"].astype(float).astype(int)
    files = {path.name for path in MARKDOWN_DIR.iterdir()} if MARKDOWN_DIR.is_dir() else set()
    df["path"] = df["dok_laufnr"].map(lambda nr: MARKDOWN_DIR / f"{nr}_docling-serve.md")
    df = df[df["path"].map(lambda path: path.name in files)]
    df["rank"] = df["titel_dok"].map(
        lambda title: next((i for i, p in enumerate(PRIORITY) if title.startswith(p)), len(PRIORITY))
    )
    return df.sort_values(["rank", "dok_laufnr"]).drop_duplicates("signatur_ges").set_index("signatur_ges")


def load_model():
    card = json.loads((MODEL_DIR / "karte.json").read_text(encoding="utf-8"))
    name = datetime.fromisoformat(card["trainiert_am"]).strftime("%Y%m%dT%H%M%S")
    return joblib.load(MODEL_DIR / "modell.joblib"), card, name


def check_embedding_server(weights: str):
    models = common.requests_get(f"{EMBED_URL}/models", headers=EMBED_HEADERS, timeout=60).json()["data"]
    offered = {m["id"]: m.get("root") for m in models}
    if EMBED_MODEL not in offered:
        raise RuntimeError(f"{EMBED_URL} does not offer {EMBED_MODEL}, only {list(offered)}")
    if weights not in (EMBED_MODEL, offered[EMBED_MODEL]):
        raise RuntimeError(f"{EMBED_MODEL} runs {offered[EMBED_MODEL]}, the model was trained on {weights}")


def read_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("<!-- page break -->", "\n").lstrip("\n")
    return text[:MAX_CHARS] or " "


def embed(texts: list[str]) -> np.ndarray:
    vectors = []
    for start in range(0, len(texts), EMBED_BATCH):
        r = common.requests_post(
            f"{EMBED_URL}/embeddings",
            json={"model": EMBED_MODEL, "input": texts[start : start + EMBED_BATCH]},
            headers=EMBED_HEADERS,
            timeout=180,
        )
        vectors += [e["embedding"] for e in sorted(r.json()["data"], key=lambda e: e["index"])]
    return normalize(np.asarray(vectors, dtype=np.float32))


def load_topics() -> pd.DataFrame:
    if not TOPICS_FILE.exists():
        return pd.DataFrame(columns=COLUMNS).set_index("signatur_ges")
    topics = pd.read_csv(TOPICS_FILE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return topics.set_index("signatur_ges")


def save(topics: pd.DataFrame):
    tmp = TOPICS_FILE.with_suffix(".csv.part")
    topics.sort_index().reset_index()[COLUMNS].to_csv(tmp, sep=";", index=False, encoding="utf-8-sig")
    tmp.replace(TOPICS_FILE)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    estimator, card, model_name = load_model()
    check_embedding_server(card["embed_model"])
    labels = get_labels()
    documents = get_documents()
    if documents.empty:
        raise RuntimeError(f"No Markdown found in {MARKDOWN_DIR}")
    topics = load_topics()
    logging.info(f"Model {model_name}, {len(labels)} labels, {len(documents)} business items with a document")

    stored = topics.reindex(labels.index)
    changed = labels.index[(labels.drop(columns="assigned_at") != stored.drop(columns="assigned_at")).any(axis=1)]
    topics = pd.concat([topics.drop(changed, errors="ignore"), labels.loc[changed].assign(assigned_at=now())])

    done = topics.index[(topics["source"] == "ogd") | (topics["model"] == model_name)]
    pending = documents.index.difference(done).sort_values()
    logging.info(f"{len(pending)} business items to classify")

    for start in range(0, len(pending), SAVE_EVERY):
        batch = pending[start : start + SAVE_EVERY]
        probabilities = estimator.predict_proba(embed([read_text(path) for path in documents.loc[batch, "path"]]))
        assigned_at = now()
        rows = {}
        for signatur, p in zip(batch, probabilities):
            ranked = [value for i in np.argsort(-p)[:TOP] for value in (card["themen"][i], f"{p[i]:.4f}")]
            dok_laufnr = str(documents.at[signatur, "dok_laufnr"])
            rows[signatur] = [*ranked, "classifier", model_name, dok_laufnr, assigned_at]
        classified = pd.DataFrame.from_dict(rows, orient="index", columns=COLUMNS[1:]).rename_axis("signatur_ges")
        topics = pd.concat([topics.drop(batch, errors="ignore"), classified])
        save(topics)
        logging.info(f"Classified {start + len(batch)} of {len(pending)}")
    save(topics)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main()
    logging.info("Job successful.")
