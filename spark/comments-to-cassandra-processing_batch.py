from __future__ import print_function
import sys
import json
import pprint
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.util import datetime_from_timestamp, uuid_from_time, unix_time_from_uuid1
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
from config import ES_HOST, CASSANDRA_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


es = Elasticsearch([ES_HOST])
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect('reddit_comments')
prepared_insert = session.prepare("""
    INSERT INTO word_time_json (
        inserted_time, id, word, created_utc_uuid, 
        created_utc, link_title, body, author, 
        subreddit, parent_id, over_18, ups, 
        downs, controversiality, gilded, score) 
    VALUES (toUnixTimestamp(now()),?,?,?,
        ?,?,?,?,
        ?,?,?,?,
        ?,?,?,?)
""")


def writeToCassandra(rdd):
    if rdd.count() > 0:
        for comment in rdd.collect():
            tmp_doc = {'doc': {'comment': comment['body']}}
            res = es.percolate(index="comment_percolators", doc_type="doctype", body=tmp_doc)
            pprint.pprint(res)
            if res['total'] > 0:
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                for matched_query in res['matches']:
                    word_query = matched_query['_id']
                    batch.add(prepared_insert, (
                        comment['id'],
                        word_query,
                        uuid_from_time(int(comment['created_utc'])),
                        int(comment['created_utc']),
                        comment['link_title'],
                        comment['body'],
                        comment['author'],
                        comment['subreddit'],
                        comment['parent_id'],
                        comment['over_18'],
                        comment['ups'],
                        comment['downs'],
                        comment['controversiality'],
                        comment['gilded'],
                        comment['score']
                    ))
                result = session.execute(batch)
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
          .foreachRDD(writeToCassandra)
    )

    ssc.start()
    ssc.awaitTermination()
