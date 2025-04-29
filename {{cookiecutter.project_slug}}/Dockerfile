# syntax=docker/dockerfile:1.4
FROM python:{{ cookiecutter.python_version }}-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Optional: install system deps
RUN apt-get update && \
    apt-get install -y \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY . /app/

{% if cookiecutter.use_poetry == 'yes' %}
# ---- Poetry Setup ----
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Install deps with Poetry
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi
{% else %}
# ---- Pip Setup ----
RUN pip install --upgrade pip && pip install -r requirements.txt
{% endif %}

# Default CMD to run consumer
CMD [ "python", "./src/main.py" ]

