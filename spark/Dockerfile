FROM bitnami/spark:3.5.0
WORKDIR /spark
COPY spark_streaming.py .
CMD ["spark-submit", \
"--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.6.0", \
"/spark/spark_streaming.py"]
