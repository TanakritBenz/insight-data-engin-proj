from __future__ import print_function
import sys, json, urllib2, base64, time, pytz
from datetime import datetime, timedelta
from pprint import pprint
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from pyspark import SparkConf
from pyspark_cassandra import CassandraSparkContext
from cassandra import ConsistencyLevel
from config import SPARK_MASTER, ES_HOST, ES_USER, ES_PWD


def formatForCassandraPostsAggTable(x):
    x = {
        'query': x[0],
        'time_utc': lasttime,
        'ups': x[1][0],
        'downs': x[1][1],
    }
    return x

if __name__ == "__main__":
    utc = pytz.UTC
    lasttime = datetime(2007, 1, 1).replace(tzinfo=utc)  # start time 01/01/06
    conf = SparkConf().setAppName("Reddit Posts Aggregate").setMaster(SPARK_MASTER)
    sc = CassandraSparkContext(conf=conf)
    while True:
        start_time = time.time()
        req = urllib2.Request(ES_HOST+'post_percolators/_search')
        base64string = base64.encodestring('%s:%s' % (ES_USER, ES_PWD)).replace('\n', '')
        req.add_header("Authorization", "Basic %s" % base64string)
        percolators = json.load(urllib2.urlopen(req))
        percolators = percolators['hits']['hits']
        percolators = [p['_id'] for p in percolators]
        rdd = (
            sc.cassandraTable("reddit", "posts")
              .select("query", "created_utc", "doc_id", "subreddit", "title", "permalink", "url", "author", "ups", "downs")
              .where("query IN ?", percolators)
        )

        rdd = (
            rdd.filter(lambda r: r["created_utc"] > lasttime and r["created_utc"] < lasttime+timedelta(hours=1))
               .map(lambda r: (r["query"], (r["ups"], r["downs"])))
               .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
               .map(formatForCassandraPostsAggTable)
        )

        print('Time: ', lasttime)
        pprint(rdd.collect())

        rdd.saveToCassandra(
            keyspace="reddit",
            table="posts_agg",
            consistency_level=ConsistencyLevel.ANY
        )
        lasttime = lasttime + timedelta(hours=1)
        print("--- Total %s seconds ---" % (time.time() - start_time))

    sc.stop()
