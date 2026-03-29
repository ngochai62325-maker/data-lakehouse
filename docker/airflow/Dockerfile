FROM apache/airflow:2.5.1-python3.9

USER root
RUN apt-get update && apt-get install -y default-jdk

USER airflow
RUN pip install pyspark delta-spark boto3 python-dotenv
