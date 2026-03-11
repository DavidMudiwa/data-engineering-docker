FROM python:3.13.11
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


WORKDIR /code
# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"
COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked
# Copy application code
COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]