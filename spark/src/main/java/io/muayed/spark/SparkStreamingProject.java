package io.muayed.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SparkStreamingProject {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Spark Streaming started now .....");

        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.createIfNotExist();

        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // batchDuration - The time interval at which streaming data will be divided into batches
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", CustomSerializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("messages");
        JavaInputDStream<ConsumerRecord<String, Performance>> messages =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Performance> Subscribe(topics, kafkaParams));


        JavaPairDStream<String, Performance> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<Performance> filtered = results
                .map(
                        tuple2 -> tuple2._2()
                ).filter(
                        p -> p.getCurrentMem() > 0
                );


        filtered.foreachRDD(rdd -> {

            System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
            if(rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {
                    hBaseUtil.put(rawRecord);
                    System.out.println(rawRecord);
                    System.out.println("***************************************");
                    System.out.println(rawRecord.getCurrentMem());


                });

            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
