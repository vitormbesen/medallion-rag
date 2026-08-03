"""Process data to generate embedding vectors."""

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer
    from torch import Tensor


def generate_embeddings(
    model: SentenceTransformer,
    chunks: list[str],
    batch_size: int = 32,
) -> np.ndarray:
    """Generates embedding vector from a chunk of text and a model."""
    return np.array(model.encode(chunks, batch_size=batch_size, normalize_embeddings=True))
