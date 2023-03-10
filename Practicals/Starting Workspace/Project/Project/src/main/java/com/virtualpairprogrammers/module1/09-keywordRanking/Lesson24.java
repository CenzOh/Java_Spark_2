//Lesson 24
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

public class Lesson24 {

	public static void main(String[] args) {
		
//Lesson 24 Continued worked solution with sorting
/* So far we have an RDD containing what we think are interesting words. Now we just have to count them. THis is just like what we did in the previous chapter. Build a pair of RDD
 * Keys are the words, values are the value of one. Then we reduce on that pair RDD where we sum up values by reducing the key
 * key    | value
 * docker | 1
 * 
 */
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
//		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt"); //can use for the spring course. Output has interesting words like spring, dao, hibernate, aop, jdbc

		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll(" ^[a-zA-Z\\s] ", "").toLowerCase()); 
		
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0); 

		JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
//at end of lessson, to fix the blank words and remove them
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));
		
/*lets start here. What we want to do is call map to pair we want the elements to map to the java tuple 2. We will have one string and the other value is a long
 * the word is the key and the number 1 as a long will be the value. This will be saved to a JavaPairRDD with string as key and long as value. Make sure to import java pair RDD if needed
 * Maybe you will have a compiler error below. If so, the problem is type mismatch cant convert java pair rdd object, object, to pair rdd string, long. 
 * This happens because we simply have to import Tuple 2. So the compiler doesnt know what types need to be here its as simple as importing.
 */		
//		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair( word -> new Tuple2<String, Long>(word, 1L));
		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair( word -> new Tuple2<String, Long>(word, 1L));


/* Now we will have an error because the Tuple business gets a little sticky. Change list type to Tuple2<String, Long>>
 * When we do the print ln we can assume there will be good string implementation for the totals.
 * OUTPUT:
 * (deployments, 1)
 * ...
 * Okay this is great. Only one instance of a blank. Now let us run a reduced by key to add up all of those ones. 
 */
		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
		
/*OUTPUT:
 * (tomcat, 93)
 * (randomly, 5)
 * (everythings, 3)
 * ...
 * A lot of low counts will NOT be a part of the big results. Now what we need to do is sort them so that the most popular terms are at the top.
 */
//		JavaPairRDD<String, Long> sorted = totals.sortByKey(); //sort by key is not really a good idea since it will sort alphabetically NOT by count. However, there is no sort by value.
//what to do is switch key and value with a simple map to pair since we are mapping to a pair of RDDs
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 )); //switching the two values around in the new tuple
		
//		JavaPairRDD<Long, String> sorted = switched.sortByKey(); //if compiler error, switch string, long to be long, string. Recall now our RDD will be 93, tomcat so we can sort by the new key
		
//At first doesnt work we have to pick descending order. Default is ascending.
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false); //add false
		
/*OUTPUT: ANd recall the text file is for a docker course
 * (857, docker)
 * (127, swarm)
 * (93, tomcat)
 * ...
 * Very nice results and relevant words to the course. We will not worry about performance for this exercise so we won't rework it but we will try to fix the  blank words
 */
		
//		List<Tuple2<String, Long>> results = pairRdd.take(50);
//		List<Tuple2<String, Long>> results = totals.take(50);
		List<Tuple2<Long, String>> results = sorted.take(50);


		results.forEach(System.out::println);
				
		sc.close();
	}

}
