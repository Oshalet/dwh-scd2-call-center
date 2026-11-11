FROM apache/airflow:2.8.3-python3.11

# Установка Java (OpenJDK 17) для PySpark
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Переменная окружения для Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements_airflow.txt /tmp/reqs.txt
RUN pip install --no-cache-dir -r /tmp/reqs.txt