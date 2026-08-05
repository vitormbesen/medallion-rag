import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

    from sentence_transformers import SentenceTransformer
    from sqlalchemy.orm import Session


def bronze_layer(title: str, logical_date: date, session: Session, user_agent: str) -> None:
    """
    Bronze layer logic.
    1. Ingests data from source (API, files, documents, etc).
    2. Persist raw data to the Bronze layer schema.
    """
    from .persistence.writing import write_to_bronze
    from .processing.bronze import fetch_document_from_api

    rows = fetch_document_from_api(title=title, logical_date=logical_date, user_agent=user_agent)
    write_to_bronze(rows=rows, session=session)


def silver_layer(logical_date: date, session: Session) -> None:
    """
    Silver layer logic.
    1. Read data from Bronze layer.
    2. Apply chunking logic.
    3. Persist chunks to Silver layer schema.
    """
    # Lazy imports
    from .persistence.reading import read_bronze_for_documents
    from .persistence.writing import write_to_silver
    from .processing.silver import chunk_documents

    # 1. Read data documents in Bronze layer that have been ingested in the respective logical date
    bronze_rows = read_bronze_for_documents(logical_date=logical_date, session=session)

    # 2. Chunk documents
    silver_rows = chunk_documents(rows=bronze_rows, logical_date=logical_date)

    # 3. Persist to Silver layer
    write_to_silver(rows=silver_rows, session=session)


def gold_layer(logical_date: date, session: Session, model: SentenceTransformer, batch_size: int = 32) -> None:
    """
    Gold layer logic.
    1. Read data from Silver layer.
    2. Apply embedding logic.
    3. Persist embeddings to gold layer schema.
    """
    # Lazy imports
    from .persistence.reading import read_silver_for_missing_embeddings_chunks
    from .persistence.writing import write_to_gold
    from .processing.gold import generate_embeddings

    # 1. Read the data from the Silver layer
    # Each row is a chunk
    silver_rows: list[dict] = read_silver_for_missing_embeddings_chunks(logical_date=logical_date, session=session)

    # 2. Apply the embedding, but batched
    # Partition data into batches
    n_batches = math.ceil(len(silver_rows) / batch_size)
    for i in range(n_batches):
        gold_rows = []
        start_idx, end_idx = batch_size * i, batch_size * (i + 1)
        batched_silver_rows = silver_rows[start_idx:end_idx]

        # Get list of chunks
        chunks: list[str] = [r['chunk_text'] for r in batched_silver_rows]

        # Generate embedding
        embeddings = generate_embeddings(model=model, chunks=chunks, batch_size=batch_size)

        # Map chunk_id, document_id and generated embedding correctly
        for silver_row, emb in zip(batched_silver_rows, embeddings, strict=True):
            gold_rows.append(
                {
                    'chunk_id': silver_row['chunk_id'],
                    'document_id': silver_row['document_id'],
                    'embedding': emb,
                    'logical_date': logical_date,
                },
            )

        # 3. Persists each batch to gold layer
        write_to_gold(rows=gold_rows, session=session)
