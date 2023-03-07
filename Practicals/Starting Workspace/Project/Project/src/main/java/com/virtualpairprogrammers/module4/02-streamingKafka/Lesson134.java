package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Lesson134 {
	
/* Lesson 134 Kafka Aggregation
 * 
 * Lets calcualte most popular course by viewing the time.
 * Output:
 * (Java FUndamentals, 8765) 
 * 
 * To do the sorting, recall we can only sort by key. Make the nums the key so we can sort by key. Swap the values around with the map
 */

	@SuppressWarnings("resource") 
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR); 
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Collection topics = Arrays.asList("viewrecords");
		
		Map<String, Object> params = new HashMap<>();
		params.put("bootstrap.servers",  "localhost:9092");
		params.put("key.deserializer",  StringDeserializer.class); //check import for stringDeserializer from kafka package.
		params.put("group.id", "spark-group");
		params.put("auto.offset.reset", "latest")

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(src, LocationStrategies.PreferConsistent(), 
				ConsumerStrategies.Subscribe(topics, params)); //pick strategies pluarl
		
//can do our usual RDD methods here
		JavaDStream<String> results = stream.map(item -> item.value());

		
		JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(), 5L)) //change to long string instead of string long with our second map to pair
				.reduceByKey( (x, y) -> x + y)
				.mapToPair(item -> item.swap()) //swap key and values so we can sort by most popular course num
				.transformToPair(rdd -> rdd.sortByKey(false)); //gives us access to underlying RDD if there is an operation not supported by stream
		
		results.print();
		
		sc.start();
		sc.awaitTermination();
	}
}