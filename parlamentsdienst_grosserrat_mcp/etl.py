import logging
import shutil
from pathlib import Path

import truststore
from corpus_mcp import job
from corpus_mcp.settings import Settings

# SQLite cannot lock files on the share: the job works in tmp/ on the Airflow host's disk, kept between
# runs, and copies to the share only what changed.
WORK = Path("tmp")
OUTPUTS = ("corpus-mcp.db", "vectors.db", "vectors.faiss", "release.json", "failed.csv")


def sync(source: Path, target: Path) -> None:
    """Copy a file whole when the target lacks it or differs; a reader sees the old file or the new one."""
    if not source.exists() or (target.exists() and same(source, target)):
        return
    partial = target.with_name(target.name + ".new")
    shutil.copy2(source, partial)
    partial.replace(target)
    logging.info("copied %s to %s", source, target)


def same(a: Path, b: Path) -> bool:
    """Same size and modification time; the share keeps times to the second only."""
    sa, sb = a.stat(), b.stat()
    return sa.st_size == sb.st_size and int(sa.st_mtime) == int(sb.st_mtime)


def main() -> None:
    share = Settings()
    local = share.model_copy(update={"data_dir": WORK / "data", "extract_db": WORK / "extract.db"})
    local.data_dir.mkdir(parents=True, exist_ok=True)
    sync(share.extract_db, local.extract_db)
    for name in OUTPUTS:
        sync(share.data_dir / name, local.data_dir / name)
    try:
        job.reindex(local)
    finally:
        for name in OUTPUTS:
            sync(local.data_dir / name, share.data_dir / name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    truststore.inject_into_ssl()
    main()
    logging.info("Job successful.")
