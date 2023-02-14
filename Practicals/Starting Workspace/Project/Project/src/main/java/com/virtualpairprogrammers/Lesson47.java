//Lesson 47
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson47 {
	
	public static void main(String[] args) {

	
		//Lesson 47 Walkthrough for Step 9
/* Step 9 - Add up total scores. Nearly finished now. We have the same RDD keyed on courseID with scores. We have to reduce by key to get total score for each course, thats our final
 * destination. (courseId, totalScore). (1, 6).
 * Note that all of this was working on local hard coded data which is a great strategy when working with these complex series of transformations.
 * We only really know if we are getting right results when we switch to some bigger data. So now we can try that by turning testMode = false around line 30.
 * Now this may not be the best way of doing things, (talking about general structure of all steps) and there may be more elegant ways of getting to the answer. This was just a simple
 * way to do it to make it understandable.
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
