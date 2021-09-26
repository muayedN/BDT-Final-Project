Spark Streaming Project
I am using Kafka producer, Spark Streaming and Hbase

1. Kafka setup
    i. I am using docker-compose to run kafka and zookeeper. docker-compose file included here.
    ii. i have used kafka command line util to create a topic named "messages". (kafka-topics.sh --create --topic messages --bootstrap-server localhost:9092)
    ii. I have a spring boot application with spring scheduler to produce data about the heap memory usage of the application. Scheduler is configured to send data to kafka
        every 1 second.

2. Spark streaming and HBase
    -- i have one java application that listens to kafka and filters the messages and send them to HBase
    -- The SparkStreamingContext is configured to flush out the messages to HBase every 20 seconds

3. Screen shots of i/p o/p are included

4. Link to video recording.  https://web.microsoftstream.com/video/9924fbf4-2737-4821-926a-5625e323343c