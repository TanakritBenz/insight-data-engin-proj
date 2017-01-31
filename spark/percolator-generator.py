from __future__ import print_function
import sys
import json
import pprint
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
from config import ES_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


es = Elasticsearch([ES_HOST])


def findMatchedQueries(rdd):
    if not rdd.isEmpty():
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
        for kv_search_pair in rdd.collect():
            # kv_search_pair => {'dog': [('puppy', 0.7), ('aww', 0.5), ('fox', 0.2) , ...]}
            keys_list = kv_search_pair.keys()
            search_terms = []
            for key in keys_list:
                search_terms.append(key)
                for child in kv_search_pair[key]:
                    search_terms.append(child[0])

            for search_term in search_terms:
                query = {
                    "query": {
                        "match": {
                            "comment": search_term
                        }
                    }
                }
                res = es.index(index='comment_percolators', doc_type='queries', body=query, id=search_term)
                pprint.pprint(res)


if __name__ == "__main__":
    sconf = SparkConf().setAppName("percolator_generator").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=sconf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "word_queries"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    queries = (
        kvs.map(lambda x: json.loads(x[1]))
        .foreachRDD(findMatchedQueries)
    )

    ssc.start()
    ssc.awaitTermination()
