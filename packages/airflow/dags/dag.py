from typing import TYPE_CHECKING

from airflow.sdk import chain, dag, get_current_context, task, task_group
import pendulum
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from sqlalchemy import Engine


def get_db_engine(conn_id: str) -> Engine:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    hook = PostgresHook(postgres_conn_id=conn_id)
    return create_engine(hook.get_uri(), poolclass=NullPool)


tz = pendulum.timezone('America/Sao_Paulo')
task_common_args = dict(  # noqa: C408
    retries=3,
    retry_delay=pendulum.duration(minutes=5),
    retry_exponential_backoff=True,
    max_active_tis_per_dagrun=1,
)

@task(task_id='init_database')
def initialize_database():
    from medallion_rag.persistence.models import init_database

    engine = get_db_engine(conn_id='db_project')
    try:
        init_database(engine)
    finally:
        engine.dispose()


@task(task_id='bronze')
def bronze_layer(title: str) -> None:
    # Lazy Imports
    from medallion_rag.pipeline import bronze_layer

    # Instantiate function dependencies
    engine = get_db_engine(conn_id='db_project')
    context = get_current_context()
    logical_date = context['ts']
    try:
        with Session(engine) as session:
            bronze_layer(
                title=title,
                session=session,
                logical_date=logical_date,
            )
    finally:
        engine.dispose()


@task(task_id='silver')
def silver_layer() -> None:
    # Lazy imports
    from medallion_rag.pipeline import silver_layer

    # Instantiate function dependencies
    engine = get_db_engine(conn_id='db_project')
    context = get_current_context()
    logical_date = context['ts']

    try:
        with Session(engine) as session:
            silver_layer(
                logical_date=logical_date,
                session=session,
            )
    finally:
        engine.dispose()


@task(task_id='gold')
def gold_layer() -> None:
    # Lazy import
    from medallion_rag.pipeline import gold_layer
    from sentence_transformers import SentenceTransformer

    # Instantiate function dependencies
    engine = get_db_engine(conn_id='db_project')
    context = get_current_context()
    logical_date = context['ts']
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

    try:
        with Session(engine) as session:
            gold_layer(
                logical_date=logical_date,
                session=session,
                model=model,
            )
    finally:
        engine.dispose()


@dag(
    dag_id='rag_population',
    description='Medallion Architecture from extraction, cleaning and vector database population.',
    schedule='0 0 * * *',
    start_date=pendulum.datetime(year=2026, month=7, day=15, tz=tz),
    end_date=pendulum.datetime(year=2026, month=12, day=31, tz=tz),
    catchup=False,
    default_args=task_common_args,
)
def rag_population() -> None:
    titles = ['Buddhism', 'Hinduism']

    db_init = initialize_database()
    bronze = bronze_layer.expand(title=titles)
    silver = silver_layer()
    gold = gold_layer()

    chain(db_init, bronze, silver, gold)


dag_object = rag_population()
