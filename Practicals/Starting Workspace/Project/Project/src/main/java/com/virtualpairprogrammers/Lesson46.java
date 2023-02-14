//Lesson 46
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson46 {
	
	public static void main(String[] args) {

	
		//Lesson 46 Walkthrough for Step 8
/* Step 8 - Score based on WCVRP. COnvert percentages into scores. All we need to do is look at business rules, very simple algorithm. So 60 odd percent will translate to score of 4.
 * Notice the transformation will preserve the keys so we can do another map values. (courseId, percent). (1, 4).
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
