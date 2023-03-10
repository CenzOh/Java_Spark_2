//Leson 25
package com.virtualpairprogrammers;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Lesson25 {
	
	public static void main(String[] args) {

	
		//Lesson 25 why do sorts not work with foreach?
/* We will address the common misconception with the sort transformation that we used in the previous chapter. We will look at the Coalesce function. 
 * Last time we built the solution which processes a (relatively) big data file. Real life can be arbitrarily big. Processes and lists top 10 important keywords. Reads the subtitles of the course
 * We avoided the common trap with the sort routine. Many people new to spark fall into the trap. Lets address it!
 * 
 * What are we talking about? Lets recap, we did a lot of transformations and ended with a sorting transformation and finish by taking first ten results of RDD. 
 * Because we already did so, that will take the ten most popular key words. HOWEVER we did NOT have to do a take!! Quite common to do a foreach
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
		
		sorted = sorted.coalesce(1); //WRONG ANSWER BUT FOR TESTING PURPOSES. Output will seem like it works and sorts correctly. 

//Lets look at the RDD after doing the sort
		sorted.foreach(System.out::println);
//		sorted.foreach(element -> System.out.println(element)); //both versions work
/* This is where common confusion arises. When we run this it won't give output we will expect. Depends on computer config to see if we see same results as instructor
 * Recall we print the whole RDD so at bottom we see some counts of 2s and 1s. But when we scroll to the top we may see it is completely out of order.
 * (1, positional)
 * (1, zips)
 * (1, subfolder)
 * (857, docker)
 * ...
 * What happened? This is a complete mess. Does seem like the numbers are descending, not totally random. Very frequent issue. 
 * By way of explaining, we will look at a WRONG explanation. One instructor has seen such as stack overflow answers and even textbooks:
 * Although we touched on intro chapter, our Spark work has the data in the RDD is divided into a number of partitions. SO partitions is just a chink of data we are working with.
 * Idea is that we can either have multiple threads working on the partitions in parallel OR we can distribute the partitions onto separate physical nodes. (Revision of what we covered earlier)
 * Continuing the WRONG explanation: we have done a sort transformation on the RDD, we might imagine we have something like this:
 * Partition 1, (1, positional), (1, zips). Partition 2, (857, docker), (127, swarm). Partition 3 (1, pretend), (1, infer). 
 * There will be a subset of the data on each of the partitions. All the sort will have done is sort the data on a partition BY a partition basis. We have been running the foreach method
 * and it appears it is going through each partition inturn and outputting the contents of each partition. At first you can buy that. Bad advice that has been given:
 * Before output results, we have to make sure all the data is shuffled into a single partition and then we would see right answers. Again, this is W R O N G. 
 * We can do it and get all the data in a single partition. The method to do this is coalesce(). Allows us to specify how many partitions we want to end up with so we can write 1 as a parameter.
 * Spark will then ensure all of the data is combined into one single partition. 
 */

		
//		List<Tuple2<Long, String>> results = sorted.take(50);
//		results.forEach(System.out::println);
				
		sc.close();
	
	}

}
