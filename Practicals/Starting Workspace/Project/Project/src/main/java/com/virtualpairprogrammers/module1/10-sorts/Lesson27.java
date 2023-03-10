//Lesson 27
package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Lesson27 {
	
	public static void main(String[] args) {

	
		//Lesson 27 What is Coalesce for?
/* Coalesce is often suggested on how to fix sorting, but in our case this is not the right answer. What is coalesce used for? When working with some real big data and we have lots
 * of partitions which are made up of 32 MB each lets say. Imagine we have been performing a lot of transformations and some actions on our multi terabyte 1000 partition RDD.
 * Later in process we are down to small level of data. Could be three or four items inside each partition. We will still have more transformations to do but it would be wasteful
 * to carry on doing those transformations across 1000 partitions. We havent talked a lot about shuffling so far but if Spark does have to shuffle any of the data around the partitions 
 * it could be slow and expensive.
 *  
 *  What we would want coalesce for is we can all this fcn on the RDD and specify the few partitions (maybe even one partition) if it can fit in some memory and continue with further transformations
 *  They will work in a more efficient fashion. RECAP - coalesce is used for PERFORMANCE reasons, NOT for corretness. A further method we may find useful is called `collect`. THis is used
 *  when we finished, we did our big data transformations, maybe spread across thousand partitions but there is not a lot of data inside the RDD. Collect will gather ALL the data and it will
 *  send it back to the driver's JVM so we can print it or do anything we want. ONLY do that if we are sure the data we have gathered would fit in RAM or single JVM!
 *  
 *  If our results are still big data, lets say gigabytes in partitions, then DONT do a collect. Maybe we would write to a file system like HDFC. When we did the take 100K before we could have 
 *  easily called collect.
 *  
 *  Again, we do NOT need to shuffle data or even know that there are partitions in your RDD to get the correct results! For operations like sorting, when we perform an action,
 *  we will get the correct result. Only exception is for each because for each runs in a multi threaded weight. OFten doesnt work the way we think it will work!
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");	
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll(" ^[a-zA-Z\\s] ", "").toLowerCase()); 
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0); 
	
		JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair( word -> new Tuple2<String, Long>(word, 1L));
		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
//		List<Tuple2<Long,String>> results = sorted.take(100000);
		List<Tuple2<Long,String>> results = sorted.collect(); //could have more easily done this, would give us same results but be careful dont run out of memory use at end of long process.

		results.forEach(System.out::println);

				
		sc.close();
	
	}

}
