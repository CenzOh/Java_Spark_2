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

public class Lesson135 {

/* Lesson 135 Adding a Window
 * 
 * Getting some botchy results, spark is falling behind a bit with our resutls. Lets add a window
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
		params.put("key.deserializer",  StringDeserializer.class); 
		params.put("group.id", "spark-group");
		params.put("auto.offset.reset", "latest")

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(src, LocationStrategies.PreferConsistent(), 
				ConsumerStrategies.Subscribe(topics, params)); 
		
//can do our usual RDD methods here
		JavaDStream<String> results = stream.map(item -> item.value());

		
		JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(), 5L)) 
				.reduceByKeyAndWindow( (x, y) -> x + y, Durations.minutes(60)) //reduce by key AND Window here, 60 min window
				.mapToPair(item -> item.swap()) 
				.transformToPair(rdd -> rdd.sortByKey(false)); 
		
		results.print();
		
		sc.start();
		sc.awaitTermination();
	}
}
