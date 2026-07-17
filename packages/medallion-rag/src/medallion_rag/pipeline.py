import math
from typing import TYPE_CHECKING

from .persistance.reading import read_bronze_for_date, read_silver_for_date
from .persistance.writing import write_to_bronze, write_to_gold, write_to_silver

# from .processing.bronze import ...
from .processing.gold import generate_embeddings

if TYPE_CHECKING:
    from datetime import date

    from sentence_transformers import SentenceTransformer
    from sqlalchemy.orm import Session


def bronze_layer():
    pass


def silver_layer():
    pass


def gold_layer(logical_date: date, session: Session, model: SentenceTransformer, batch_size: int = 32):
    # Each row is a chunk
    silver_rows: list[dict] = read_silver_for_date(logical_date=logical_date, session=session)

    # Partition data into batches
    n_batches = math.ceil(len(silver_rows) / batch_size)
    for i in range(n_batches):
        gold_rows = []
        start_idx, end_idx = batch_size * i, batch_size * (i + 1)
        batched_silver_rows = silver_rows[start_idx:end_idx]

        # Get list of chunks
        chunks: list[str] = [r['chunk_text'] for r in batched_silver_rows]

        # Generate embedding
        embeddings = generate_embeddings(model=model, chunks=chunks)

        # Map chunk_id, document_id and generated embedding correctly
        for silver_row, emb in zip(batched_silver_rows, embeddings, strict=True):
            gold_rows.append(
                {
                    'chunk_id': silver_row['chunk_id'],
                    'document_id': silver_row['document_id'],
                    'embedding': emb,
                    'updated_at': logical_date,
                },
            )

        # Persists to gold layer
        write_to_gold(rows=gold_rows, session=session)
