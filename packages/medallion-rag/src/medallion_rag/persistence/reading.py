"""Reads persisted data from previous layer for processing."""

from typing import TYPE_CHECKING

from sqlalchemy import select

from .models import DocumentEmbedding, ProcessedChunk, RawDocument

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session



def read_bronze_for_documents(logical_date: date, session: Session) -> list[dict]:
    """
    Read bronze rows for a given logical_date.
    Utilized to obtain data for the silver layer processing.
    """
    # Read the documents ingested in the particular logical_date
    stmt = select(
        RawDocument.document_id,
        RawDocument.payload,
        RawDocument.extracted_at,
    ).where(RawDocument.logical_date == logical_date)
    return [dict(row) for row in session.execute(stmt).mappings()]


def read_silver_for_missing_embeddings_chunks(logical_date: date, session: Session) -> list[dict]:
    """
    Read silver chunks for the given `date`.
    Utilized to obtain data for the gold layer processing.
    """
    # Select the chunks which are not present in the Gold layer.
    # That is, if the document is already embedded, there is no need
    # redo the calculation.
    stmt = (
        select(
            ProcessedChunk.chunk_id,
            ProcessedChunk.document_id,
            ProcessedChunk.chunk_text,
        )
        .outerjoin(DocumentEmbedding, DocumentEmbedding.chunk_id == ProcessedChunk.chunk_id)
        .where(
            DocumentEmbedding.chunk_id.is_(None),
            ProcessedChunk.logical_date == logical_date,
        )
    )

    return [dict(row) for row in session.execute(stmt).mappings()]
