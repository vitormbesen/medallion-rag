"""Processing data from the Bronze layer for persistence on the Silver layer."""

import hashlib
from typing import TYPE_CHECKING

from nltk.tokenize import sent_tokenize

if TYPE_CHECKING:
    from datetime import date


# TODO: airflow: `get_updated_docs`, apply expand on chunking + writing
def get_unique_chunk_id(document_id: int | str, chunk_idx: int, chunk_text: str) -> str:
    """Generate a unique chunk_id, which is used as primary key."""
    return hashlib.sha256(f'{document_id}:{chunk_idx}:{chunk_text}'.encode()).hexdigest()


def chunk_documents(rows: list[dict], logical_date: date) -> list[dict]:
    # Each row consists of document_id and payload.
    silver_rows = []
    for r in rows:
        doc_id = r['document_id']

        # Get chunks
        chunks = sent_tokenize(r['payload'].get('text'))
        for idx, chunk in enumerate(chunks):
            silver_rows.append(
                {
                    'document_id': doc_id,
                    'chunk_id': get_unique_chunk_id(doc_id, idx, chunk),
                    'chunk_index': idx,
                    'chunk_text': chunk,
                    'logical_date': logical_date,
                },
            )

    return silver_rows
