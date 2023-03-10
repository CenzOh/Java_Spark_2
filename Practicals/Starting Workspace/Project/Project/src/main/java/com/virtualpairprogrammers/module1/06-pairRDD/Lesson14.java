//Lesson 14
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 

public class Lesson14 {

	public static void main(String[] args) {
		
/* Lesson 14 Overview of Pair RDDs. COmmon to have a pair of values. So we can have a key and value pair. Advantage is having extra methods in a pair RDD such as grouping by key
 * New example: processing log files for a busy server
 * WARN: Tuesday 4 September 0405
 * ERROR: Tuesday 4 September 0408
 * 
 * simple format, left side is level of logging while right side (after colon) we have date and time log was produced. Goal, count how many warnings, errors, and fatals.
 * input testing data:
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
		
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData); //change to RDD of strings
/* How can we count up how many warnings, errors, etc. To count num, we need to split this file into two components, the level (left side) and date field (right side)
 * Recall tuples. Maybe we make a new RDD based on original, use map method, containing the tuples. Elements are string, string. Which are level, date and time. Group related lvs together
 * Sounds good right? But when we make a new RDD how will we do the counting? Several ways like making a new RDD to filter out warnings, another for just fatals, etc. then run ocunt.
 * COuld work but not a good general solution because if there were thousands of different types, too many RDDs!! 
 * Turns out common thing to do is separate into two columns. A Pair RDD. PairRDD <String, String> has Key and Value, can visualize as table with 2 columns.
 * Seems like a java map. But there is a difference. Notice, the key repeats with different value, look at WARN. In Spark, this is fine. Can have multiple instances of same key
 * similar to a bag structure. THis is very useful. because we could have gotten the same results with tuples but pair RDDs give us extra methods to use
 */
	
		sc.close();
	}

}
