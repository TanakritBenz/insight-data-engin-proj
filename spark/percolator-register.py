from __future__ import print_function
import sys
import json
import pprint
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import ES_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3

def registerQuery(query):
    es = Elasticsearch([ES_HOST])
    mapping = {
        "mappings": {
            "doctype": {
                "properties": {
                    "comment": {
                        "type": "text"
                    }
                }
            },
            "queries": {
                "properties": {
                    "query": {
                        "type": "percolator"
                    }
                }
            }
        }
    }
    es.indices.create(index='comment_percolators', body=mapping, ignore=400)
    query = {
        "query": {
            "match": {
                "comment": query
            }
        }
    }
    res = es.index(index='comment_percolators', doc_type='queries', body=query, id=query)
    pprint.pprint(res)


if __name__ == "__main__":
    sconf = SparkConf().setAppName("percolator_generator").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=sconf)
    ssc = StreamingContext(sc, 1)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "word_queries"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    queries = (
        kvs.map(lambda x: x[1])
           .filter(lambda r: len(r) > 0)
           .foreachRDD(lambda r: r.foreach(registerQuery))
    )

    ssc.start()
    ssc.awaitTermination()
