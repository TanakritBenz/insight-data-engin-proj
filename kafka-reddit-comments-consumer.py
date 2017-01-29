from __future__ import print_function


import sys
import json
import pprint

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from elasticsearch import Elasticsearch
es = Elasticsearch(['http://elastic:changeme@ec2-35-163-28-86.us-west-2.compute.amazonaws.com:9200/'])

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
    sconf = SparkConf().setAppName("reddit_comments_consumer").setMaster("spark://ip-172-31-0-109:7077")
    sc = SparkContext(conf=sconf)
    ssc = StreamingContext(sc, 5)
    brokers = "ec2-52-34-22-125.us-west-2.compute.amazonaws.com:9092,ec2-52-33-253-180.us-west-2.compute.amazonaws.com:9092,ec2-52-34-64-163.us-west-2.compute.amazonaws.com:9092"
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
