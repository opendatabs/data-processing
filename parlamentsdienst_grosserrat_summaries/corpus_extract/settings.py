"""Where to read, where to write and whom to ask."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """The settings of the job: Gemma from the DAG's GEMMA_* variables, the rest from EXTRACT_*."""

    model_config = SettingsConfigDict(env_prefix="EXTRACT_", extra="ignore", populate_by_name=True)

    data_dir: Path = Field(default=Path("data"), description="Where extract.db is written.")
    corpus_dir: Path = Field(default=Path("data_orig"), description="The Markdown and Docling JSON, read-only.")
    mcp_db: Path = Field(
        default=Path("data_orig/mcp/corpus-mcp.db"), description="The database of parlamentsdienst_grosserrat_mcp."
    )
    llm_url: str = Field(validation_alias="GEMMA_URL", description="Gemma's OpenAI API, including /v1.")
    llm_model: str = Field(validation_alias="GEMMA_MODEL", description="The model id as /v1/models lists it.")
    llm_api_key: str = Field(default="", validation_alias="GEMMA_API_KEY", description="The gateway's key.")
    limit: int = Field(default=500, description="Documents per run.")
    concurrency: int = Field(default=8, description="Requests at once.")

    @property
    def db_path(self) -> Path:
        """The database this job owns."""
        return self.data_dir / "extract.db"


def get_settings() -> Settings:
    """The settings as the environment sets them."""
    return Settings()
