from typing import TYPE_CHECKING

from airflow.sdk import dag, get_current_context, task
import pendulum
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from sqlalchemy import Engine


# ———————————————————————————————————————————————————————
# Shared objects
# ———————————————————————————————————————————————————————
tz = pendulum.timezone('America/Sao_Paulo')
task_common_args = dict(
    retries=3,
    retry_delay=pendulum.duration(minutes=5),
    retry_exponential_backoff=True,
    max_active_tis_per_dagrun=4,
)


# ———————————————————————————————————————————————————————
# Utility functions
# ———————————————————————————————————————————————————————
def get_db_engine(conn_id: str) -> Engine:
    """Return postgres database engine."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    hook = PostgresHook(postgres_conn_id=conn_id)
    return create_engine(hook.get_uri(), poolclass=NullPool)


def get_engine_and_logical_date() -> tuple[Engine, pendulum.DateTime]:
    """Return postgres database engine and current DAG timestamp logical date."""
    engine = get_db_engine(conn_id='db_project')
    context = get_current_context()
    logical_date = context['logical_date']
    return engine, logical_date


# ———————————————————————————————————————————————————————
# Initialize Database function
# ———————————————————————————————————————————————————————


@task(task_id='init_database')
def initialize_database() -> None:
    from medallion_rag.persistence.models import init_database

    engine = get_db_engine(conn_id='db_project')
    try:
        init_database(engine)
    finally:
        engine.dispose()


# ———————————————————————————————————————————————————————
# Read DAG configuration -- topics, model, etc
# ———————————————————————————————————————————————————————
def load_config() -> dict:
    from pathlib import Path

    import omegaconf

    path = Path('/opt/airflow/config/medallion_rag_config.yaml')
    cfg = omegaconf.OmegaConf.load(path)
    # return omegaconf.OmegaConf.to_container(cfg, resolve=True)
    return cfg


# ———————————————————————————————————————————————————————
# Bronze layer
# ———————————————————————————————————————————————————————


@task(task_id='bronze')
def bronze_layer(title: str, user_agent: str) -> None:
    # Lazy Imports
    from medallion_rag.pipeline import bronze_layer

    # Instantiate function dependencies
    engine, logical_date = get_engine_and_logical_date()
    try:
        with Session(engine) as session:
            bronze_layer(
                title=title,
                session=session,
                logical_date=logical_date,
                user_agent=user_agent,
            )
    finally:
        engine.dispose()


# ———————————————————————————————————————————————————————
# Silver Layer
# ———————————————————————————————————————————————————————
@task(task_id='silver')
def silver_layer() -> None:
    # Lazy imports
    from medallion_rag.pipeline import silver_layer

    # Instantiate function dependencies
    engine, logical_date = get_engine_and_logical_date()

    try:
        with Session(engine) as session:
            silver_layer(
                logical_date=logical_date,
                session=session,
            )
    finally:
        engine.dispose()


# ———————————————————————————————————————————————————————
# Gold Layer
# ———————————————————————————————————————————————————————
@task(task_id='gold')
def gold_layer(model_name: str, batch_size: int = 32) -> None:
    # Lazy import
    from medallion_rag.pipeline import gold_layer
    from sentence_transformers import SentenceTransformer

    # Instantiate function dependencies
    engine, logical_date = get_engine_and_logical_date()
    model = SentenceTransformer(model_name)

    try:
        with Session(engine) as session:
            gold_layer(
                logical_date=logical_date,
                session=session,
                model=model,
                batch_size=batch_size,
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

    # Read configuration
    config = load_config()

    # Initialize database
    db_init = initialize_database()

    # Run Medallion layers
    bronze = (
        bronze_layer.partial(user_agent=config.bronze.user_agent).expand(  # constant value
            title=config.bronze.titles
        )  # dynamic value
    )
    silver = silver_layer()
    gold = gold_layer(model_name=config.gold.model, batch_size=config.gold.batch_size)

    # Define execution order
    db_init >> bronze >> silver >> gold


# Instantiate DAG object
dag_instance = rag_population()
