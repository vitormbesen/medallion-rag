"""
will read from bronze table, extract data, apply chunking
and write to silver table.

only references the payload.

bronze (payload)
silver (reads payload from bronze, transform into chunks)
"""

import hashlib
from typing import TYPE_CHECKING

from nltk.tokenize import sent_tokenize

if TYPE_CHECKING:
    from datetime import date


def get_unique_chunk_id(document_id: int, chunk_idx: int, chunk_text: str):
    """Generate a unique chunk_id, which is used as primary key."""
    return hashlib.sha256(f'{document_id}:{chunk_idx}:{chunk_text}'.encode()).hexdigest()


def chunk_documents(rows: list[dict], logical_date: date):
    # Each row consists of document_id and payload.
    silver_rows = []
    for r in rows:
        doc_id = r['document_id']

        # Get chunks
        chunks = sent_tokenize(r['payload'])
        for idx, chunk in enumerate(chunks):
            silver_row = dict(  # noqa: C408
                document_id=doc_id,
                chunk_id=get_unique_chunk_id(doc_id, idx, chunk),
                chunk_index=idx,
                chunk_text=chunk,
                logical_date=logical_date,
            )
            silver_rows.append(silver_row)

    return silver_rows
