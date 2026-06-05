# Stage 1: Base image với Python
FROM python:3.11-slim as base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

RUN apt-get update && \
	apt-get install -y --no-install-recommends \
		nano \
        openjdk-21-jre-headless && \
	rm -rf /var/lib/apt/lists/*

# Dependencies
COPY requirements.in /app

RUN uv pip compile requirements.in -o requirements.txt && \
    uv pip install \
		--no-cache-dir \
		--system \
		-r requirements.txt
