FROM bitnami/spark:3.3.0

USER root
RUN apt-get update && apt-get install -y curl
RUN curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar
RUN curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
