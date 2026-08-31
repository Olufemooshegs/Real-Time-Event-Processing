FROM flink:1.19.3-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && python3 -m pip install --no-cache-dir apache-flink==1.19.3 psycopg2-binary \
    && curl --fail --location --silent --show-error \
      --output /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar \
https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar \
    && curl --fail --location --silent --show-error \
      --output /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
    && curl --fail --location --silent --show-error \
      --output /opt/flink/lib/postgresql-42.7.3.jar \
https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY jobs /opt/flink/usrlib/jobs
RUN chown -R flink:flink /opt/flink/usrlib

ENV PYTHON_EXECUTABLE=/usr/bin/python3
ENV PYTHON_CLIENT_EXECUTABLE=/usr/bin/python3

USER flink
