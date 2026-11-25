import logging
import shutil
from pathlib import Path

import truststore
from corpus_extract import job
from corpus_extract.settings import get_settings

# SQLite cannot lock files on the share: the job reads in tmp/ on the Airflow host's disk, kept between
# runs, and copies extract.db to the share when it changed.
WORK = Path("tmp")


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
    share = get_settings()
    local = share.model_copy(update={"data_dir": WORK})
    WORK.mkdir(exist_ok=True)
    sync(share.db_path, local.db_path)
    try:
        job.main(local)
    finally:
        sync(local.db_path, share.db_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    for client in ("httpx", "httpx2"):
        logging.getLogger(client).setLevel(logging.WARNING)
    truststore.inject_into_ssl()
    main()
    logging.info("Job successful.")
