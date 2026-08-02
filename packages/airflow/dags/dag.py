from typing import TYPE_CHECKING

from airflow.sdk import chain, dag, task
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


@task(task_id='bronze')
def bronze_layer() -> None:
    pass


@task(task_id='silver')
def silver_layer() -> None:
    pass


@task(task_id='gold')
def gold_layer() -> None:
    engine = get_db_engine(conn_id='db_project')
    try:
        with Session(engine) as session:
            ...
            # function logic receiving session
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
    bronze = bronze_layer()
    silver = silver_layer()
    gold = gold_layer()

    chain(bronze, silver, gold)


dag_object = rag_population()
