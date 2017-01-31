from __future__ import print_function
import sys
import json
import pprint
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from ..config import ES_HOST, CASSANDRA_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


es = Elasticsearch([ES_HOST])


def filterEmptyRDD(rdd):
    return not rdd.isEmpty()


def writeToES(rdd):
    es.indices.create(index='reddit_comments', ignore=400)
    for comment in rdd.collect():
        print('comment json =>', pprint.pprint(comment))
        res = es.index(index="reddit_comments", doc_type='reddit_comment_json', body=comment)
        print('comment wrote to es =>', pprint.pprint(res))
        print('\n')

if __name__ == "__main__":
    sconf = SparkConf().setAppName("reddit_comment_consumer").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=sconf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "reddit_comments"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    comments = (
        kvs.map(lambda x: json.loads(x[1]))
           .filter(filterEmptyRDD)
    )
    comments.pprint(3)
    comments.foreachRDD(writeToES)

    ssc.start()
    ssc.awaitTermination()
