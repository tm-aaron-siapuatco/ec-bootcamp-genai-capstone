FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV DAGSTER_HOME=/opt/dagster/dagster_home
WORKDIR /opt/dagster/app

# Create Dagster Home directory
RUN mkdir -p $DAGSTER_HOME

# Copy official 'uv' binary directly from astral-sh
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 1. Copy pipeline source code & workspace configuration
COPY . /opt/dagster/app/

# 2. Ensure dagster.yaml is placed directly in DAGSTER_HOME
COPY dghome/dagster.yaml $DAGSTER_HOME/dagster.yaml

# 3. Install packages into system python
RUN uv pip install --system -e .

EXPOSE 3000