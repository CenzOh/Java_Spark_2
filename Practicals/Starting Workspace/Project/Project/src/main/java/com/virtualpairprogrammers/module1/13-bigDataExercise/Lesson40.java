//Lesson 40
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson40 {
	
	public static void main(String[] args) {

	
		//Lesson 40 Walkthrough for Step 2
/* Step 2 - join to get Course ID in the RDD. At this point we have removed the duplicates. WE have the viewData table with userId and chapterId. However, none of this is useful since
 * We do not know from this RDD if chapter 96 is the same course as chapter 97. We need to get the course Ids into this RDD. How? Recall we have another RDD called chapterData
 * which includes chapterId and courseId. We have to join these two tables on the chapterId. We cant do it yet since the viewData table is keyed on userId. So we have to swap the cols.
 * Then we can do an inner join on the key. 
 * 
 * We want to end up with this:
 * chapterId | (userId, courseId)
 * 96		 | (14, 1)
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		sc.close();
	
	}
	
}
