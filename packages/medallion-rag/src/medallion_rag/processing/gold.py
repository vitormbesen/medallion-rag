"""Process data to generate embedding vectors."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


def generate_embeddings(
    model: SentenceTransformer,
    chunks: list[str],
    batch_size: int = 32,
):
    return model.encode(chunks, batch_size=batch_size, normalize_embeddings=True)
