FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --default-timeout=1000 --retries=10 --no-cache-dir -r /tmp/requirements.txt