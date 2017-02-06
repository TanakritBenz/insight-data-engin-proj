#!/bin/bash

spark-submit \
    --master spark://ip-172-31-0-108:7077 \
    --executor-memory 6G \
    --driver-memory 6G \
    --packages TargetHolding/pyspark-cassandra:0.3.5,org.apache.spark:spark-streaming-kafka_2.10:1.6.2 \
    --conf spark.cassandra.connection.host=ec2-35-164-1-15.us-west-2.compute.amazonaws.com,ec2-35-164-114-69.us-west-2.compute.amazonaws.com,ec2-35-167-223-98.us-west-2.compute.amazonaws.com,ec2-52-24-219-40.us-west-2.compute.amazonaws.com \
    /home/ubuntu/insight-data-engin-proj/spark/posts-processing.py
