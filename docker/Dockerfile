FROM apache/airflow:slim-3.2.2-python3.14

# Install uv binary
USER root
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        git \
        build-essential \
 && rm -rf /var/lib/apt/lists/*

ENV UV_PYTHON=/usr/local/bin/python \
    UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy

# Copy medallion-rag + airflow package dependencies
COPY packages /opt/packages

# Install the uv airflow project (which already contains medallion-rag)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system /opt/packages/airflow

USER airflow
WORKDIR /opt/airflow