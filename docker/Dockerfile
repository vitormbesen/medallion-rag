FROM apache/airflow:3.2.2-python3.14

# uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy package code
COPY --chown=airflow:root ./medallion-rag /opt/airflow/medallion-rag

# Package installation in Airflow environment requires root
USER root
COPY ./medallion-rag/pyproject.toml /tmp/medallion-rag/

# 4. CACHED LAYER: Install dependencies directly into the system Python
# uv will resolve and download everything here. This layer caches heavily.
RUN uv pip install --system --no-cache-dir -r /tmp/medallion-rag/pyproject.toml

# 5. UNCACHED LAYER: Copy the actual source code (changes frequently)
COPY --chown=airflow:root ./medallion-rag /opt/airflow/medallion-rag

# 6. Install the package itself (fast because dependencies are already present)
RUN uv pip install --system --no-cache-dir --no-deps /opt/airflow/medallion-rag

# 7. Switch back to safe airflow runtime user
USER airflow

