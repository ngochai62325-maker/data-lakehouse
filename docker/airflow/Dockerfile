FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --default-timeout=1000 --retries=10 --no-cache-dir -r /tmp/requirements.txt