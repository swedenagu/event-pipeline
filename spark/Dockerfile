FROM apache/spark-py:v3.4.0
USER root
ENV PATH="/opt/spark/bin:${PATH}"
WORKDIR /opt/spark/spark
COPY spark_streaming.py .
COPY schema/user_event_v1.avsc .
RUN chown -R 185:0 /opt/spark/spark && chmod g+w /opt/spark/spark
USER 185
CMD ["spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.spark:spark-avro_2.12:3.4.0,org.apache.httpcomponents.client5:httpclient5:5.3.1,org.apache.httpcomponents.core5:httpcore5:5.2.4", \
    "/opt/spark/spark/spark_streaming.py"]
