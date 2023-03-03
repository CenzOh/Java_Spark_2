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

public class Lesson136 {

/* Lesson 136 Adding a Slide Interval
 * 
 * Relationship between batch size and the window size which is a whole hour, batch is one second. Widnwo size must be multiple of batch size. If we had batch of 5 seconds, we cant
 * have a window of 7 seconds. There is a tuning option called slide itnerval. This determines how often should data be dumped for the report? What weve been running we recieved output
 * every second. What if we want a report every minute? We can achieve that making batch size 60 seconds or one minute., This will work. Downside is batch size SO MUCH BIGGER. 
 * 
 * Work done witll have more work to be done to produce the aggregation. So we may wait longer than 60 seconds. Makes sense to have small batch size.
 * So we added the third param to reduce by key and winow. SO we;ll have output every minute. 60 rdds will be built. 
 * 
 * We encountered a nasty bug. Kafka not safe for multi threaded acecss. Strange but issue tracker for Spark did document this issue. 
 * https://issues.apache.org/jira/browse/SPARK-19185 
 * THis bug has thankfully been fixed. In the pom.xml, just change ALL the versions to 2.4.0 this fixes the error. Do this for all dependencies. DO eclipse:clean, rerun like usual.
 * Another issue with some versions of eclipse. Can see warning that check validation failed. Maven setting got corrupt from a simple upgrade. Refresh project and we get error
 * with issues with jar files. Fix for this is to delete the .m2 directory, the maven cache where the home folder is. Run pom again. Repeatable issue so something may be wrong in eclipse. 
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
				.reduceByKeyAndWindow( (x, y) -> x + y, Durations.minutes(60), Durations.minutes(1)) //third param is for our slide interval
				.mapToPair(item -> item.swap()) 
				.transformToPair(rdd -> rdd.sortByKey(false)); 
		
		results.print();
		
		sc.start();
		sc.awaitTermination();
	}
}
