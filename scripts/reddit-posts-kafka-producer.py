import sys
import time
import json
import boto3, botocore
from random import randint
from pprint import pprint
from kafka import KafkaProducer
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


class Producer(object):

    def __init__(self, ip_addr):
        self.producer = KafkaProducer(bootstrap_servers=ip_addr, value_serializer=lambda m: json.dumps(m))

    def __get_s3_bucket__(self, bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                print (e.response['404 Error: bucket not found'])
            else:
                print (e.response['Error'])
            return None
        else:
            return s3.Bucket(bucket_name)

    def produce_msgs(self):
        wanted_keys = [u'id', u'title', u'permalink', u'url',
                       u'author', u'subreddit', u'created_utc',
                       u'over_18', u'ups', u'downs', u'gilded',
                       u'score']
        msg_cnt = 1
        bucket = self.__get_s3_bucket__('reddit-submissions-split')
        for obj in bucket.objects.all():
            if msg_cnt == 1:
                start_time = time.time()
            obj_body = obj.get()['Body']
            json_body = obj_body.read().splitlines()
            for json_obj in json_body:
                json_obj = json.loads(json_obj)
                try:
                    msg = {wanted_keys: json_obj[wanted_keys] for wanted_keys in wanted_keys}
                    msg['downs'] = randint(0,msg['ups'])
                    for m_tuple in msg.iteritems():
                        if isinstance(m_tuple[1], basestring):
                            msg[m_tuple[0]] = m_tuple[1].lower()
                except KeyError:
                    # A few jsons don't have all the wanted keys
                    msg = {}
                    continue
                if (msg_cnt == 1):
                    pprint(msg)
                self.producer.send('reddit-posts', msg)
                msg_cnt = msg_cnt+1
                if (msg_cnt % 1250 == 0):
                    time.sleep(1)
                    print(str(msg_cnt), ' msg sent')
                    pprint(msg)
                    print("---  %s seconds ---" % (time.time() - start_time))
            print('Sent ' + str(msg_cnt) + ' messages so far')
            time.sleep(2)
        print('Total ' + str(msg_cnt) + ' messages were sent!')


if __name__ == "__main__":
    kafka_ips = [KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3]
    prod = Producer(kafka_ips)
    prod.produce_msgs()
