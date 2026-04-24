FROM python:3.13.11
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app
# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml .python-version uv.lock ./

COPY 3_data-warehouse/load_data.py .

RUN uv sync --locked
# Copy application code
#COPY ingest_data.py .
# Set entry point
ENTRYPOINT ["python", "load_data.py"]