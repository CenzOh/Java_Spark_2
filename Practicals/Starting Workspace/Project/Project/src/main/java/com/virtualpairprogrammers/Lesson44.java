//Lesson 44
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson44 {
	
	public static void main(String[] args) {

	
		//Lesson 44 Walkthrough for Step 6
/* Step 6 - How many chapters? So far this is great. FOr every row, we know that somebody watched course 1 and watched two chapters. Recall that our business rules are based on what
 * proportion of the course they watched. We cant tell right now. We did this already in the warmup exercise! (courseId, chapters). (1,3). All we have to do is to join the two
 * RDDs based on courseID for resulting: (courseID, (views, of)). (1, (2,3)). THis means someone watched course 1, viewed 2 chapters out of a total of 3 chapters. 
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
