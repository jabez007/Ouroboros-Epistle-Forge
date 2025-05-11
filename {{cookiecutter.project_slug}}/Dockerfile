# syntax=docker/dockerfile:1.4
FROM python:{{ cookiecutter.python_version }}-slim as base

LABEL maintainer="{{ cookiecutter.author_name}} <{{ cookiecutter.author_email}}>"
LABEL description="{{ cookiecutter.project_description }}"
LABEL version="0.1.0"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser

{% if cookiecutter.use_poetry == 'yes' %}
# Optional: install system deps
RUN apt-get update -y && \
    apt-get install -y \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ---- Poetry Setup ----
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy pyproject first for better caching
COPY pyproject.toml /app/

# Install deps with Poetry
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi
{% else %}
# ---- Pip Setup ----
# Copy requirements first for better caching
COPY requirements.txt /app/

# Install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
{% endif %}

# Clean up to reduce image size
RUN apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy project files with appropriate ownership
COPY --chown=appuser:appuser . /app/

USER appuser

# Default CMD to run consumer
ENTRYPOINT [ "python" ]
CMD [ "/app/src/main.py" ]

# Health check - assumes your application responds to SIGTERM for graceful shutdown
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "python /app/src/main.py" || exit 1
