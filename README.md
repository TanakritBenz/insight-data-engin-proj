
#justRDDit (read "just-reddit")

##### A web application that allows you to search Reddit posts in real time. 

#Introduction

Let's be real; it is very hard to make it to Reddit frontpage. Not only your post has to have a _compelling content_, you also need an _appealing title_ to go with it. In order to overcome this, I built justRDDit. A tool that aims to help people come up with more ideas to better construct Reddit post titles. That way our posts can rake in more _upvotes_ and eventually climb to Reddit frontpage!

#Pipeline

![alt text][logo]

[logo]: https://github.com/TanakritBenz/justrddit/raw/master/images/pipeline.png "Pipeline"

Reddit posts data is read from __S3__ then fed them to 3 nodes of __Kafka__. 4 Nodes of __Spark Streaming__ consume the formatted data from __Kafka__ and "percolate" them through __Elasticsearch Percolators__ in order to categorise and put them into "buckets" in __Cassandra__. Another __Spark Batch__ job (residing in the same cluster as the first job) read from __Cassandra__ the processed data from the first job, then it does mapreduce to get aggregated sum of upvotes and downvotes. It then writes the stats into another __Cassandra__ table. Last but not least, __Node.js__ endpoints read from these 2 tables and __AngularJS__ dynamically updates the results showing to users.

#####AWS Machines:
* __3__ nodes of m3.medium (Kafka)
* __4__ nodes of m3.large (Spark)
* __4__ nodes of m4.large (Databases)

#Results
###Performance
The system is able to easily process more than 1000 of records per second with less than 4 seconds total delay. The throughout could be pused higher but it would result in a longer delay. The amount of posts being produced on actual Reddit is way less than what justRDDit can handle. My decision to keep it at this rate is because I prefered to have my frontend be as close to real time as possible.
