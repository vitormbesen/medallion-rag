FROM apache/airflow:slim-3.2.2-python3.14

USER root

# 1. Install uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. Install essential C-compilation tools & Postgres client headers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow/app
ENV UV_SYSTEM_PYTHON=1

# 3. Copy packages directory
COPY packages /opt/airflow/app

# 4. Force install psycopg2-binary, fab provider, and local workspace packages
RUN uv pip install \
    --index-strategy unsafe-best-match \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    psycopg2-binary \
    apache-airflow-providers-fab \
    -e /opt/airflow/app/medallion-rag \
    -e /opt/airflow/app/airflow

# 5. Fix permissions and switch to airflow user
RUN chown -R airflow:root /opt/airflow/app
USER airflow