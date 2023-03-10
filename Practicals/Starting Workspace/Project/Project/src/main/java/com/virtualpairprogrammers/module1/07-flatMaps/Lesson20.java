//Lesson 20
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 

public class Lesson20 {

	public static void main(String[] args) {
		
// Lesson 20 Filters
/* Lets quickly look at another useful fcn, filter. With this fcn, we can take an RDD and transform it by removing any values we are NOT interested in.
 * ex - lets say we want to remove any entry which is just of length 1, such as the single numerical values. Filter OUT of the collection.
 */
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> sentences = sc.parallelize(inputData); //first make an RDD out of our input which rn are sentences
		
		JavaRDD<String> words = sentences.flatMap( value -> Arrays.asList(value.split(" ")).iterator()); //will convert into a regular list of strings. Standard java. Next compile error we see is
		
		JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1); //magic happens here. Filter iterates around every element, which are now single words. SO given a word, if return true
/* become part of new RDD and if false, filtered out of RDD / not present. Can test it out and just add true or false to show if it works. Here we are saying if the word is a length of
 * 1 or less, it will not print out and it works. 
 * Also reminder we do not ahve to do all of these separate operations on separate lines. Can do all of this on one single line. WOuld look like this:
 */
		
//		sc.parallelize(inputData)
//			.flatMap( value -> Arrays.asList(value.split(" ")).iterator())
//			.filter(word -> word.length() > 1)
//			.foreach(System.out::println);
		
		filteredWords.foreach(System.out::println);
//ERROR:
//Friday
//September
//..

		sc.close();
	}

}
