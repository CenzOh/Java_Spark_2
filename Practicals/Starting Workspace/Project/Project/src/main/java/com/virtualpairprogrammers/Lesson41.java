//Lesson 41
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson41 {
	
	public static void main(String[] args) {

	
		//Lesson 41 Walkthrough for Step 3
/* Step 3 - Lose the chapter ID
 * Ok now we have the RDD with chapterId (userId, courseId) 96 (14,1). Each row in the RDD represents a view of a course. We know that the views are distinct based on user ID and chapter ID.
 * That was the only reason we needed the chapter ID. We dont need it anymore. We know that from the first row that a user watched a chapter from course one then the same user watched
 * a different chapter from course one. No need to look at the key column. Just look at (14, 1). ChapterID has become redundant, we only needed it at the beginning to ensure everything
 * was distinct. Now we can drop it. Let us transform the RDD now so that the userID and courseID become the key. Then we want the value to be 1 so we can count how many chapters a 
 * user watched.
 * 
 * (userId, courseId) | count
 * (14,1 )			  | 1
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
