from __future__ import print_function
import sys, json, copy, time, datetime
from pprint import pprint
import pyspark_cassandra
from cassandra import ConsistencyLevel
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch, ConnectionError
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import ES_HOST_1, ES_HOST_2, ES_HOST_3, ES_HOST_4, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3



def expandRDDbyMatch(x):
    res = []
    for match in x[1]['matches']:
        tmp_dict = copy.copy(x[1])
        tmp_dict['query'] = match[u'_id']
        res.append((x[0], tmp_dict))
    return res


def limitMatchesAt5(x):
    if x[1]['match_total'] > 5:
        x[1]['match_total'] = 5
        x[1]['matches'] = x[1]['matches'][:5]
    return x


def percolatePost(x):
    es = Elasticsearch([ES_HOST_1, ES_HOST_2, ES_HOST_3, ES_HOST_4], max_retries=20)
    tmp_doc = {'doc': {'post': x[1]['title']}}
    percol_res = es.percolate(index="post_percolators", doc_type="doctype", body=tmp_doc)
    x[1]['percol_res'] = percol_res
    try:
        x[1]['match_total'] = x[1]['percol_res']['total']
        x[1]['matches'] = x[1]['percol_res']['matches']
        del x[1]['percol_res']
    except KeyError:
        pass
    return x


def formatForCassandraPostsTable(x):
    x = {
        'query': x[1]['query'],
        'created_utc': datetime.datetime.fromtimestamp(float(x[1]['created_utc'])),
        'doc_id': x[0],
        'subreddit': x[1]['subreddit'],
        'title': x[1]['title'],
        'permalink': x[1]['permalink'],
        'url': x[1]['url'],
        'author': x[1]['author'],
        'ups': int(x[1]['ups']),
        'downs': int(x[1]['downs']),
    }
    return x


def processRDD(rdd):
    start_time = time.time()
    rdd = (
        rdd.map(lambda x: (x['id'], x))
           .reduceByKey(lambda x,y: x) #discard similar posts
           .filter(lambda x: x[1]['over_18'] == False)
           .map(percolatePost)
           .filter(lambda x: x[1]['match_total'] > 0)
           .map(limitMatchesAt5)
           .flatMap(expandRDDbyMatch)
           .map(formatForCassandraPostsTable)
    )
    print("--- Compute time: %s seconds ---" % (time.time() - start_time))

    start_time2 = time.time()
    rdd.saveToCassandra(
        keyspace="reddit",
        table="posts",
        consistency_level=ConsistencyLevel.ANY
    )

    print("--- Save to Cassandra: %s seconds ---" % (time.time() - start_time2))

    print("--- Total %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    conf = SparkConf().setAppName("Reddit Posts Digest").setMaster(SPARK_MASTER)
    conf.set("spark.shuffle.io.maxRetries", 20)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "reddit-posts"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    posts = (
        kvs.map(lambda x: json.loads(x[1]))
           .filter(lambda r: len(r) > 0)
           .foreachRDD(processRDD)
    )

    ssc.start()
    ssc.awaitTermination()
