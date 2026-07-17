"""Reads persisted data from previous layer for processing."""

from typing import TYPE_CHECKING

from sqlalchemy import select

from .models import ProcessedChunk, RawDocument

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.orm import Session


def read_bronze_for_date(logical_date: date, session: Session) -> list[dict]:
    """
    Read bronze rows for a given logical_date.
    Utilized to obtain data for the silver layer processing.
    """
    stmt = select(
        RawDocument.document_id,
        RawDocument.payload,
        RawDocument.extracted_at,
    ).where(RawDocument.logical_date == logical_date)
    return [dict(row) for row in session.execute(stmt).mappings()]


def read_silver_for_date(logical_date: date, session: Session) -> list[dict]:
    """
    Read silver chunks for the given `date`.
    Utilized to obtain data for the gold layer processing.
    """
    stmt = select(
        ProcessedChunk.chunk_id,
        ProcessedChunk.document_id,
        ProcessedChunk.chunk_text,
    ).where(ProcessedChunk.processed_at == logical_date)
    return [dict(row) for row in session.execute(stmt).mappings()]
