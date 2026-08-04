"""CLI to test ANN search against the gold layer."""

from __future__ import annotations

import argparse
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from medallion_rag.search import search_chunks


def main() -> None:
    p = argparse.ArgumentParser(description='Test ANN search sobre gold.document_embeddings.')
    p.add_argument('--query', type=str, help='Query do usuário')
    p.add_argument('--top-k', type=int, default=5)
    p.add_argument('--ef-search', type=int, default=40, help='HNSW ef_search (recall/speed knob).')
    p.add_argument(
        '--db-uri',
        type=str,
        default='postgresql+psycopg2://admin:admin@localhost:5433/dbproject',
    )
    p.add_argument(
        '--model',
        type=str,
        default='sentence-transformers/all-MiniLM-L6-v2',
    )
    p.add_argument('--explain', action='store_true', help='Print EXPLAIN ANALYZE of the search query.')
    args = p.parse_args()

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(args.model)

    engine = create_engine(args.db_uri)

    with Session(engine) as session:
        # Sanity: make sure the table has data.
        n = session.execute(text('SELECT count(*) FROM gold.document_embeddings;')).scalar()
        if n == 0:
            print('gold.document_embeddings is empty. Run the rag_population DAG first.', file=sys.stderr)
            sys.exit(1)
        print(f'gold.document_embeddings: {n} rows\n')

        if args.explain:
            qvec = model.encode([args.query], normalize_embeddings=True)[0]
            vec_lit = '[' + ','.join(f'{x:.6f}' for x in qvec.tolist()) + ']'
            explain_sql = text(f"""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
                SELECT chunk_id
                FROM gold.document_embeddings
                ORDER BY embedding <#> '{vec_lit}'::vector
                LIMIT {args.top_k};
            """)
            print('--- EXPLAIN ANALYZE ---')
            for row in session.execute(explain_sql):
                print(row[0])
            print('-----------------------\n')

        results = search_chunks(
            query=args.query,
            model=model,
            session=session,
            top_k=args.top_k,
            ef_search=args.ef_search,
        )

        for i, r in enumerate(results, 1):
            print(f'[{i}] similarity={r["similarity"]:.4f}  doc_id={r["document_id"][:10]}…')
            print(r['chunk_text'][:400].replace('\n', ' '))
            print('-' * 80)


if __name__ == '__main__':
    main()