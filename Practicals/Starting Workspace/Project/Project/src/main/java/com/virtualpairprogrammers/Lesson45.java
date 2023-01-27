//Lesson 45
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Lesson45 {
	
	public static void main(String[] args) {

	
		//Lesson 45 Walkthrough for Step 7
/* Step 7 - Percentage of Course? Probably easiest of all steps. COnvert these into percentages for the algorithm. Simply divide one by the other. Create an RDD with Int for courseID and
 * doubles for percentages. (courseId, percent). (1, 0.6667). Well not really a percentage but a ratio but thats okay.
 * So we would use a map to do this. HOWEVER, if we are doing a map on a pair RDD and we are NOT changing the keys, we can use a convenient method called map value. This means we only
 * have to think about the value in the Java code and it makes things easier. 
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
