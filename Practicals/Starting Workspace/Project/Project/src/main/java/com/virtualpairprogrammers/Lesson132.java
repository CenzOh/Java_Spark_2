package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class Lesson132 {

/* Lesson 132 Integrate Kafka with Spark
 * 
 * We setup the private kafka server with prewritten code to simulate viewing records of online video. Every 5 seconds when viewer watches video, report will be sent to us.
 * Now lets see how the spark streaming job would work. What if we want a chart of most watches courses. For now we arent running the view reports simulator. Write a new
 * class in streaming package called ViewingFiguresDStreamVersion
 * 
 * In dependencies for kafka we have artifact id as spark-streaming-kafka-0-10_2.11 
 */
	@SuppressWarnings("resource") 
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR); 
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
	}
}