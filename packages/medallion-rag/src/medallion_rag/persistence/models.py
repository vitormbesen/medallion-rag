"""Source of truth for schemas and tables for SQL database."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from pgvector.sqlalchemy import Vector
from sqlalchemy import TIMESTAMP, ForeignKey, Integer, String, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

if TYPE_CHECKING:
    from sqlalchemy import Engine


def init_database(engine: Engine) -> None:
    """Creates schemas, if they are do not exist."""
    with engine.connect() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS bronze;'))
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS silver;'))
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS gold;'))

        conn.execute(text('CREATE EXTENSION IF NOT EXISTS vector;'))

        conn.commit()

    BaseSchemaTable.metadata.create_all(bind=engine)

class BaseSchemaTable(DeclarativeBase):  # noqa: D101
    pass


class RawDocument(BaseSchemaTable):
    """Bronze layer table."""

    __tablename__ = 'raw_documents'
    __table_args__ = {'schema': 'bronze'}  # noqa: RUF012

    document_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    extracted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    logical_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class ProcessedChunk(BaseSchemaTable):
    """Silver layer table."""

    __tablename__ = 'processed_chunks'
    __table_args__ = (
        UniqueConstraint('document_id', 'chunk_index', name='uq_document_chunk'),
        {'schema': 'silver'},
    )

    chunk_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    document_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey('bronze.raw_documents.document_id', ondelete='CASCADE'),
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    logical_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class DocumentEmbedding(BaseSchemaTable):
    """Gold layer table."""

    __tablename__ = 'document_embeddings'
    __table_args__ = {'schema': 'gold'}  # noqa: RUF012

    chunk_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey('silver.processed_chunks.chunk_id', ondelete='CASCADE'),
        primary_key=True,
    )
    document_id: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(384), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    logical_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
