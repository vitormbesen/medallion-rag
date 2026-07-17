"""Source of truth for schemas and tables for SQL database."""

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import TIMESTAMP, Date, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawDocument(Base):
    """Bronze layer table."""

    __tablename__ = 'raw_documents'
    __table_args__ = {'schema': 'bronze'}
    document_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    extracted_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    logical_date: Mapped[datetime] = mapped_column(Date, nullable=False)


class ProcessedChunk(Base):
    """Silver layer table."""

    __tablename__ = 'processed_chunks'
    __table_args__ = {'schema': 'silver'}
    chunk_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    document_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey('bronze.raw_documents.document_id', ondelete='CASCADE'),
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class DocumentEmbedding(Base):
    """Gold layer table."""

    __tablename__ = 'document_embeddings'
    __table_args__ = {'schema': 'gold'}
    chunk_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey('silver.processed_chunks.chunk_id', ondelete='CASCADE'),
        primary_key=True,
    )
    document_id: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(384), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
