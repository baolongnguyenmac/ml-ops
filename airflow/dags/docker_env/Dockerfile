FROM python:3.11-slim as base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

RUN apt-get update && \
	apt-get install -y --no-install-recommends \
		nano \
		curl \
        openjdk-21-jre-headless && \
	rm -rf /var/lib/apt/lists/*

# Dependencies
COPY requirements.in /app
RUN uv pip compile requirements.in -o requirements.txt && \
    uv pip install \
        --no-cache-dir \
        --system \
        -r requirements.txt

# Spark dependencies
ENV SPARK_PACKAGES="io.trino:trino-jdbc:480,\
io.delta:delta-spark_4.1_2.13:4.3.0,\
org.apache.hadoop:hadoop-aws:3.4.2,\
software.amazon.awssdk:bundle:2.29.52,\
org.postgresql:postgresql:42.7.11"
RUN spark-submit --packages "${SPARK_PACKAGES}" \
    /dev/null 2>/dev/null || true
