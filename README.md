### Processing transit data 

#### Requirements
* `pip install requirements.txt`
* Download the latest Apache Kafka binary version [here](https://kafka.apache.org/downloads) and extract in `kafka/` (this project used version 2.11-2.4.1)
* Download and install the latest Apache Spark version [here](https://spark.apache.org/downloads.html)

#### Running the system
* `kafka/kafka_2.11-2.4.1/bin/zookeeper-server-start.sh kafka/kafka_2.11-2.4.1/config/zookeeper.properties`
* `kafka/kafka_2.11-2.4.1/bin/kafka-server-start.sh kafka/kafka_2.11-2.4.1/config/server.properties`
* `python src/stream.py`
* `spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.1 src/spark_script.py`

Marvin Straathof\
10353860\
University of Amsterdam