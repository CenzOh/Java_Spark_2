//Lesson 43
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson43 {
	
	public static void main(String[] args) {

	
		//Lesson 43 Walkthrough for Step 5
/* Step 5 - Drop the User Id. At this point we dont need the user ID any further. All we need to know is that someone watched course one and watched two chapters from it.
 * Thats the only thing important to us now. If someone else came along and watched course one just once and so on. Simply things for following result:
 * courseId, views. 1, 2. Do this with simple map
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
