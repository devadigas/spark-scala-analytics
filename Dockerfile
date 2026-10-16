FROM apache/airflow:3.1.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openjdk-17-jdk-headless \
    && curl -fsSL https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt \
        -o /usr/local/bin/sbt \
    && chmod 0755 /usr/local/bin/sbt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
