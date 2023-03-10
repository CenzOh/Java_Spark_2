//Lesson 52
package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Lesson52 {
	
	public static void main(String[] args) {

	
		//Lesson 52 Dealing with Key Skew
/* THis is a problem common in SPark. WE have a job and lets say it takes half an hour to run. But we want to get it running in 10 mins. Maybe we invest in expanding cluster size. Paying
 * for expensive instances and doesnt improve performance. Likely reason, we have an issue with all data ending on just one single node or a small num of nodes (like the last exercise).
 * Join operations work on keys and they are wide transformation recall that the data will be shuffled thats why its a wide transformation. If we dont have a wide spread of keys we can
 * end up with massive join RDDs residing on one node. Can run into out of memory exceptions. Doing work after the join will be harder and take longer.
 * 
 * How to avoid this? FIrst suggestion, rework scripts so the wide transformations occur LATER. Think of group bys and joins working on SMALL subset of data that can run quick.
 * If not possible? Resort to salting our keys. It is a simple hack but makes our scripts more complicated. THis is a last resort method. 
 * Using our last example, we get ALL our WARNs in ONE partition. What we could have done earlier, is change the keys so that the key is the log level, WARN, followed by a random number.
 * This number is called the salt. Ex - one partition has (WARN1, [Saturday 8 ...]). Another partition has (WARN2, [Saturday 8...]). WARN1s will be all in one cluster. Note we will have 
 * multiple WARN1s. THis will have our WARNs neatly spread across the entire cluster. 
 * 
 * We can do further transformations. Lets say we need to count all the WARNs. So we count all WARN1s, WARN2s, etc. Add the sub totals together. SOunds like map reduce.
 * Salting, in other words, mangles the keys to artificially spread them across the partitions. It means we have to, at some point, do the work to group them back together.  
 * Again, this is a last resort method.
 * 
 * In our code, recall we ended up for stage two and had all the data on two partitions and we had nine partitions. If we used an 11 node cluster, 9 would be doing nothing.
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
	//Q: How will initialRdd be partitioned?
	//A: 64mb blocks (at time of recording) so about 6 partitions
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");	
	//key: log level
	//value: date
		System.out.println("Initial RDD Partition SIze: " + initialRdd.getNumPartitions()); 
		
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair( inputLine -> {
			String[] cols = inputLine.split(":");
			String level = cols[0] + (int)(Math.random() * 11); //this is grabbing a random number and adding it to the end of our level such as `WARN6`.
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		//Now we are going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey(); //expensive operation with lots of shuffling
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2)+ " elements"));
/*our output now displays all the levels and we can see how they are spread evenly
 *  key ERROR7 has 445719 elements
 *  key WARN0 has 454415 elements
 *  
 *  Instead of ONE key for each level we have MULTIPLE keys for each level
 *  on spark UI, if we look at stage 1, it was all very skewed but this time, duration min is 0.7 and max is 7s. We can also see tasks are pretty balanced. 2s, 4s, 0,7s, 7s. All 
 *  11 partitions read at least some of the data. Their shuffle read size 3.6 mb, 8.4 mb, 3.7 mb,, etc.
 */
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		

		sc.close();
	
	}
	
}
