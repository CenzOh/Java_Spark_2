package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Lesson140 {

/* Lesson 140 Window and Watermarks
 * 
 * Waht if want figures for last hours? Remember window operation from DStream. Structured Streaming different tho. Before we did this where when we performed aggregation we
 * used alternative version of aggretation method which ended the window so we can specify a period. In sql we aggregate through group by clause. We can add a tfurther grouping
 * called window. 
 * 
 * In the window after group by you write the literal. 1 minute, 5 minutes, 2 minutes etc.
 * Section in user guide to exlain handling late data and watermarking. If event arrives late, spark structured streaming WILL include it in the window even tho it was closed.
 * We need intermediate amount of data it would need to store would accumulate. Simple config really. 
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR); 
		
//boilerplate ^^^^^^
		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("structuredViewingReport")
				.getOrCreate();
		
		Dataset<Row> df = session.readStream() 
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe",  "viewrecords")
				.option("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.load(); 
		
//start dataframe ops
		df.createOrReplaceTempView("viewing_figures");
		
//key, value, timestamp
		Dataset<Row> results = session.sql("select window, cast (value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp, '1 minute') course_name"); 
		
		StreamingQuery query = results 
			.writeStream()
			.format("console")
			.outputMode(OutputMode.Update()) 
			.option("truncate", false)
			.option("numRows", 50)
			.start(); 
		
		query.awaitTermination();
	}
}