# syntax=docker/dockerfile:1.4
FROM python:{{ cookiecutter.python_version }}-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser

# Optional: install system deps
RUN apt-get update && \
    apt-get install -y \
        curl \
    && rm -rf /var/lib/apt/lists/*

{% if cookiecutter.use_poetry == 'yes' %}
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
RUN pip install --upgrade pip && pip install -r requirements.txt
{% endif %}

# Copy project files
COPY . /app/

USER appuser

# Default CMD to run consumer
CMD [ "python", "./src/main.py" ]

