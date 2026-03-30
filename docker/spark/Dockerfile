FROM apache/spark:3.5.8

USER root

RUN curl -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar && \
    curl -o /opt/spark/jars/delta-storage-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt