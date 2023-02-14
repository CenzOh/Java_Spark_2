//Lesson 42
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson42 {
	
	public static void main(String[] args) {

	
		//Lesson 42 Walkthrough for Step 4
/* Step 4 - Count views for User / Course. Input RDD to set the views. Different order from before, what we have is: (userId, courseID), views. (14, 1), 1. Again we know they are different
 * chapters. Run a reduce so we can get: (userId, courseId), views. (14,1), 2. So after reduce we get total count of views. No more duplicates
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
