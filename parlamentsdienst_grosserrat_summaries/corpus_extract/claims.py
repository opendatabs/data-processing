"""What the model reads from a document — and what of it survives."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from corpus_extract.vocabulary import ALLOWED, EntityType, Predicate

MIN_QUOTE = 8
MAX_QUOTE = 600
# What a broken answer is made of: ": ", ":::", ": null", ":[".
PLACEHOLDER_CHARS = " :[]"


class Relation(BaseModel):
    """A relation stated in the text, with the sentence it rests on."""

    # Field order is generation order: the quote before the claim it supports.
    quote: str = Field(min_length=MIN_QUOTE, max_length=MAX_QUOTE, description="Verbatim from the document.")
    predicate: Predicate
    object_type: EntityType
    object_name: str = Field(min_length=2, max_length=200, description="The name as it appears in the document.")

    @field_validator("object_name")
    @classmethod
    def strip_edges(cls, value: str) -> str:
        """Strip whitespace and quotation marks from the edges of the name."""
        return value.strip().strip('"«»')

    @property
    def allowed(self) -> bool:
        """Whether this predicate may point at this kind of thing."""
        return self.object_type in ALLOWED[self.predicate]


class Sweep(BaseModel):
    """What the model says about a document."""

    summary: str = Field(default="", max_length=1200, description="2-4 sentences about the whole document.")
    core_demand: str = Field(default="", max_length=600, description="The core in one or two sentences.")
    # Room for every demand and question of a long Interpellation, as the prompt asks for each one.
    measures: list[str] = Field(default_factory=list, max_length=30)
    questions: list[str] = Field(default_factory=list, max_length=50)
    arguments: list[str] = Field(default_factory=list, max_length=10)
    relations: list[Relation] = Field(default_factory=list, max_length=8)

    @field_validator("summary", "core_demand")
    @classmethod
    def no_placeholder(cls, value: str) -> str:
        """Empty is allowed; punctuation alone (": ", ":::", ": null") is a broken answer."""
        if value and value.strip(PLACEHOLDER_CHARS).lower() in ("", "null"):
            raise ValueError(f"placeholder instead of text: {value!r}")
        return value

    def quoted(self, body: str) -> list[Relation]:
        """Only the relations whose quote really appears in the document and that are allowed."""
        flattened = " ".join(body.split())
        return [r for r in self.relations if r.allowed and " ".join(r.quote.split()) in flattened]
