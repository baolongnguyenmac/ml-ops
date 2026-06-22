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

# Download JAR files for Spark's connection
# # Postgres: postgresql-42.7.11.jar
# # Hive + AWS: hadoop-aws-3.4.2.jar, bundle-2.29.52.jar
# # Trino: trino-jdbc-480.jar
# # Delta table format: delta-*.jar
RUN mkdir -p /app/spark/jar && \
    curl -fSL -o /app/spark/jar/postgresql-42.7.11.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar && \
    curl -fSL -o /app/spark/jar/hadoop-aws-3.4.2.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar && \
    curl -fSL -o /app/spark/jar/bundle-2.29.52.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar && \
    curl -fSL -o /app/spark/jar/trino-jdbc-480.jar \
        https://repo1.maven.org/maven2/io/trino/trino-jdbc/480/trino-jdbc-480.jar && \
    curl -fSL -o /app/spark/jar/delta-spark_4.1_2.13-4.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.2.0/delta-spark_4.1_2.13-4.2.0.jar && \
    curl -fSL -o /app/spark/jar/delta-storage-4.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/4.2.0/delta-storage-4.2.0.jar && \
    curl -fSL -o /app/spark/jar/delta-kernel-api-4.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-kernel-api/4.2.0/delta-kernel-api-4.2.0.jar && \
    curl -fSL -o /app/spark/jar/delta-kernel-defaults-4.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-kernel-defaults/4.2.0/delta-kernel-defaults-4.2.0.jar
