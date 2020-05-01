# Task-CogKnit
Fetching data from audio files and manipulating it using Kafka, Hbase, SOLR and Spark

In order to run this code we first need to create a topic, Hello-Kafka, using the following command on our Kafka server (This is for Windows system):

    ./kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Hello-Kafka
