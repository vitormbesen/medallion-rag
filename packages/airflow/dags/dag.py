from airflow.sdk import chain, dag, task
import pendulum

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
    pass


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
