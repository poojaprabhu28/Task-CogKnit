# Task-CogKnit
This Java application fetches audio metadata from .csv file and pushes it into Kafka through a Kafka Producer

Prerequisites:
1. Create Kafka topics with topic_name as follows- "Rap", "Pop", "Rock, "Acoustic, "Unknown-genre"
    Use the following command to do so (This command applies to Windows OS. Please check for the corresponding commands if any other OS is being   used)
    ./kafka-topics.bat --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic topic_name

2. Use the sample data file (audio_data.txt) provided and insert the related filepath to it before running the application

3. Software and versions used:
    Java - version 1.8.0_191
    Apache Kafka - version 2.5.0
    slf4j simple - version 1.7.30

