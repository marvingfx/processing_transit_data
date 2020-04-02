from pyspark import SparkConf, SparkContext, storagelevel
from pyspark.streaming import kafka, StreamingContext


def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def print_offset_ranges(rdd):
    for o in offsetRanges:
        print('topic: {}\npartition: {}\nfromOffset: {}\nuntilOffset: {}'.format(o.topic, o.partition, o.fromOffset, o.untilOffset))


if __name__ == '__main__':
    spark_conf = SparkConf()
    spark_conf.setAppName('PySparkStreamApplication')
    spark_context = SparkContext.getOrCreate(conf=spark_conf)
    spark_streaming_context = StreamingContext(sparkContext=spark_context, batchDuration=2)

    brokers = 'localhost:9092,0.0.0.0:2181'
    topics = ['trip_updates', 'alerts', 'vehicle_positions']
    directKafkaStream = kafka.KafkaUtils.createDirectStream(
        spark_streaming_context,
        topics,
        {"metadata.broker.list": brokers}
    )

    directKafkaStream \
        .transform(store_offset_ranges) \
        .persist(storagelevel.StorageLevel(useDisk=True, useMemory=False, useOffHeap=False, deserialized=True))\
        .foreachRDD(print_offset_ranges)

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()
