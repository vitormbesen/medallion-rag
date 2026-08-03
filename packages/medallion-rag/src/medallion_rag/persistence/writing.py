"""Functions to write to specific tables."""

from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert as pg_insert

from .models import BaseSchemaTable, DocumentEmbedding, ProcessedChunk, RawDocument

if TYPE_CHECKING:
    from sqlalchemy import Insert
    from sqlalchemy.orm import Session


def build_upsert_dict(
    insert_stmt: Insert,
    model_class: type[BaseSchemaTable],
    custom_cols: dict[str, object] | None = None,
) -> dict:
    """Generates the set_ dictionary for SQLAlchemy PostgreSQL upsert for ensuring idempotency."""
    if custom_cols is None:
        custom_cols = {}

    return {
        col.name: insert_stmt.excluded[col.name]  # pyright: ignore[reportAttributeAccessIssue]
        for col in model_class.__table__.columns
        if col.name not in custom_cols
    } | custom_cols


def write_to_bronze(rows: list[dict], session: Session) -> None:
    """Write provided data to bronze layer."""
    # Upsert logic for idempotency
    stmt = pg_insert(RawDocument).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['document_id'],
        set_=build_upsert_dict(stmt, RawDocument),
    )
    session.execute(stmt)
    session.commit()


def write_to_silver(rows: list[dict], session: Session) -> None:
    """Write provided data to silver layer."""
    # Upsert logic for idempotency
    stmt = pg_insert(ProcessedChunk).values(rows)
    upsert_dict = build_upsert_dict(
        stmt,
        ProcessedChunk,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=['chunk_id'],
        set_=upsert_dict,
    )
    session.execute(stmt)
    session.commit()


def write_to_gold(rows: list[dict], session: Session) -> None:
    """Write provided data to gold layer."""
    stmt = pg_insert(DocumentEmbedding).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=['chunk_id'])

    session.execute(stmt)
    session.commit()
