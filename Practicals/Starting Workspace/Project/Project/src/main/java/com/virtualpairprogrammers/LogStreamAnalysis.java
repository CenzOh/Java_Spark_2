package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamAnalysis {

	@SuppressWarnings("resource") 
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR); 
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");

		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2)); 

		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);

		JavaDStream<String> results = inputData.map(item -> item); 

		JavaPairDStream<String, Long> pairDStream = results.mapToPair(rawLogMessage -> new Tuple2<> (rawLogMessage.split(",")[0], 1L)); 
		
		pairDStream = pairDStream.reduceByKeyAndWindow((x,y) -> x + y, Durations.minutes(2));  //use reduceByKeyAndWindow, it takes additional parameters such as the window duration
//we will do 2 minutes for now to show what happens when we have enough info to fill the window
		
		pairDStream.print();
		
		sc.start();
		sc.awaitTermination();
		
/* -------------------------------------------
Time: 1677869234000 ms
-------------------------------------------
(DEBUG,44269)
(FATAL,10)
(WARN,7923)
(ERROR,920)
(INFO,35383)

-------------------------------------------
Time: 1677869236000 ms
-------------------------------------------
(DEBUG,44313)
(FATAL,9)            
(WARN,7940)
(ERROR,926)
(INFO,35457)
 */
	}
}