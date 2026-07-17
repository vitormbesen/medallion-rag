"""Functions to write to specific tables."""

import datetime
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert as pg_insert

from .models import DocumentEmbedding, ProcessedChunk, RawDocument

if TYPE_CHECKING:
    from sqlalchemy import Insert
    from sqlalchemy.orm import DeclarativeBase, Session


def build_upsert_dict(
    insert_stmt: Insert,
    model_class: DeclarativeBase,
    custom_cols: dict[str, object] | None = None,
):
    """Generates the set_ dictionary for SQLAlchemy PostgreSQL upsert for ensuring idempotency."""
    if custom_cols is None:
        custom_cols = {}

    return {
        col.name: insert_stmt.excluded[col.name] for col in model_class.__table__.columns if col.name not in custom_cols
    } | custom_cols


def write_to_bronze(rows: list[dict], session: Session) -> None:
    """Write provided data to bronze layer."""
    document_ids = {r['document_id'] for r in rows}
    for doc_id in document_ids:
        # Delete if already exists, ensuring idempotency
        session.query(RawDocument).filter_by(document_id=doc_id).delete()
    stmt = pg_insert(RawDocument).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['document_id'],
        set_=build_upsert_dict(stmt, RawDocument),
    )
    session.execute(stmt)
    session.commit()


def write_to_silver(rows: list[dict], session: Session) -> None:
    """Write provided data to silver layer."""
    document_ids = {r['document_id'] for r in rows}
    for doc_id in document_ids:
        # Delete if already exists, ensuring idempotency
        session.query(ProcessedChunk).filter_by(document_id=doc_id).delete()
    stmt = pg_insert(ProcessedChunk).values(rows)
    upsert_dict = build_upsert_dict(
        stmt,
        RawDocument,
        custom_cols={'processed_at': datetime.datetime.now()},
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=['document_id'],
        set_=upsert_dict,
    )
    session.execute(stmt)
    session.commit()


def write_to_gold(rows: list[dict], session: Session) -> None:
    """Write provided data to gold layer."""
    document_ids = {r['document_id'] for r in rows}
    for doc_id in document_ids:
        # Delete if already exists, ensuring idempotency
        session.query(DocumentEmbedding).filter_by(document_id=doc_id).delete()
    stmt = pg_insert(DocumentEmbedding).values(rows)
    session.execute(stmt)
    session.commit()
