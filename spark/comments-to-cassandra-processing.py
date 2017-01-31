from __future__ import print_function
import sys
import json
import pprint
from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from ..config import ES_HOST, CASSANDRA_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


es = Elasticsearch([ES_HOST])
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect('reddit_comments')
prepared_insert = session.prepare(
    """
    INSERT INTO word_time_json (
            id,
            word,
            created_utc,
            link_title,
            body,
            author,
            subreddit,
            parent_id,
            over_18,
            ups,
            downs,
            controversiality,
            gilded,
            score
        )
        VALUES (
            id=?,
            word=?,
            created_utc=?,
            link_title=?,
            body=?,
            author=?,
            subreddit=?,
            parent_id=?,
            over_18=?,
            ups=?,
            downs=?,
            controversiality=?,
            gilded=?,
            score=?
        );
"""
)


def filterEmptyRDD(rdd):
    return not rdd.isEmpty()


def writeToCassandra(rdd):
    for comment in rdd.collect():
        tmp_doc = {'doc': {'comment': comment['body']}}
        res = es.percolate(index="comment_percolators", doc_type="doctype", body=tmp_doc)
        pprint.pprint(res)

        if res['total'] > 0:
            for matched_query in res['matches']:
                word_query = matched_query['_id']
                result = session.execute([
                    matched_query['id'],
                    word_query,
                    matched_query['created_utc'],
                    matched_query['link_title'],
                    matched_query['body'],
                    matched_query['author'],
                    matched_query['subreddit'],
                    matched_query['parent_id'],
                    matched_query['over_18'],
                    matched_query['ups'],
                    matched_query['downs'],
                    matched_query['controversiality'],
                    matched_query['gilded'],
                    matched_query['score']
                ])
                pprint.pprint(result)


if __name__ == "__main__":
    conf = SparkConf().setAppName("Reddit Comment Cassandra Processor").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "reddit_comments"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    comments = (
        kvs.map(lambda x: json.loads(x[1]))
           .filter(filterEmptyRDD)
           .foreachRDD(writeToCassandra)
    )

    ssc.start()
    ssc.awaitTermination()
