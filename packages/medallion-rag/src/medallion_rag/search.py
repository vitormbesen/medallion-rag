"""Semantic search over the gold layer using pgvector ANN."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from .persistence.models import DocumentEmbedding, ProcessedChunk

if TYPE_CHECKING:
    import numpy as np
    from sqlalchemy.orm import Session
    from sentence_transformers import SentenceTransformer


def search_chunks(
    query: str,
    model: SentenceTransformer,
    session: Session,
    top_k: int = 5,
    ef_search: int | None = None,
) -> list[dict]:
    """
    Return the top-k chunks most similar to `query`.
    """
    if ef_search is not None:
        session.execute(select(1).execution_options())
        from sqlalchemy import text
        session.execute(text(f'SET LOCAL hnsw.ef_search = {int(ef_search)};'))

    query_vec = model.encode([query], normalize_embeddings=True)[0]

    distance = DocumentEmbedding.embedding.max_inner_product(query_vec).label('distance')
    
    # Distance retrieives chunk ids. We need to query the silver layer for actual text
    stmt = (
        select(
            ProcessedChunk.chunk_id,
            ProcessedChunk.document_id,
            ProcessedChunk.chunk_text,
            distance,
        )
        .join(
            DocumentEmbedding,
            DocumentEmbedding.chunk_id == ProcessedChunk.chunk_id,
        )
        .order_by(distance)
        .limit(top_k)
    )

    rows = [dict(r) for r in session.execute(stmt).mappings()]
    for r in rows:
        # Convert negated inner product → similarity
        r['similarity'] = -r.pop('distance')
    return rows