"""Where the re-index reads and writes."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from corpus_mcp import embeddings


class Settings(BaseSettings):
    """The settings of the re-index, from MCP_* in the environment."""

    model_config = SettingsConfigDict(env_prefix="MCP_", extra="ignore")

    data_dir: Path = Field(default=Path("data"), description="Parlament/mcp: the database and the vectors.")
    corpus_dir: Path = Field(default=Path("data_orig"), description="The Markdown and Docling JSON, read-only.")
    extract_db: Path = Field(
        default=Path("data_orig/summaries/extract.db"), description="Gemma's summaries, from the summaries job."
    )
    topics: Path = Field(default=Path("data_orig/topics/topics.csv"), description="The topics job's topics.csv.")
    embed: bool = Field(default=True, description="Whether the re-index embeds the passages it has no vectors for.")
    embed_limit: int = Field(default=0, description="Documents embedded per run; 0 means every one that needs it.")
    embed_threads: int = Field(default=0, description="CPU threads for embedding; 0 means every core.")

    def embedder(self) -> embeddings.OnnxEmbedder:
        """The embedding model on the CPU."""
        return embeddings.OnnxEmbedder(threads=self.embed_threads)

    @property
    def db_path(self) -> Path:
        """The database the MCP server reads."""
        return self.data_dir / "corpus-mcp.db"

    @property
    def vectors_path(self) -> Path:
        """The passage vectors, kept apart from the database so a re-index does not discard them."""
        return self.data_dir / "vectors.db"

    @property
    def index_path(self) -> Path:
        """The nearest-neighbour index over the passage vectors, built from them after embedding."""
        return self.data_dir / "vectors.faiss"

    @property
    def release_path(self) -> Path:
        """Written when the database or the vectors changed: the folder is complete."""
        return self.data_dir / "release.json"

    @property
    def failed_path(self) -> Path:
        """The documents a step could not handle, rewritten by each re-index."""
        return self.data_dir / "failed.csv"


def get_settings() -> Settings:
    """The settings as the environment sets them."""
    return Settings()
