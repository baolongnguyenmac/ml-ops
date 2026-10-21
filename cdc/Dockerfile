FROM cnfldemos/cp-server-connect-datagen:0.6.4-7.6.0

USER root

# Debezium PostgreSQL CDC (Confluent Hub)
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4

# Confluent JDBC sink (flat Avro from Flink)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0

# Debezium JDBC sink is not on Confluent Hub — install from Maven Central
ARG DEBEZIUM_VERSION=2.5.4.Final
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-jdbc \
  && curl -fsSL \
    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/${DEBEZIUM_VERSION}/debezium-connector-jdbc-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-jdbc --strip-components=1

USER appuser
