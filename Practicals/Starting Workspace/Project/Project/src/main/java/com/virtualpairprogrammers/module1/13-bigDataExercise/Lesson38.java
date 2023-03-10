//Lesson 38
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson38 {
	
	public static void main(String[] args) {

	
		//Lesson 38 Warmup prior to starting the hard stuff
/* FIrst objective is to build an RDD containing a key of courseId together with the number of chapters on that course.
 * So we intake chapterData -> output will be:
 * courseId | chapters
 * 1		| 3
 * 2		| 1
 * 3		| 10
 * 
 * Since beginning we have had this java file called ViewingFigures.java Please refer to it now. Quick note is that in this file all the boring routine work. In it, we have the main method
 * which has got the usual start up for spark like context, etc. We also have a couple lines of code which set up the initial RDDs, one for view data (userID and chapterID, most important one)
 * Another RDD called chapterData (the one we are about to start working on with chapterID, courseID) and a less important one called titlesData to allow us to find out the names of the 
 * courses so we can print the results at the end. Notice how the methods are private. Take note about what we are doing for instance, there is one method called set Up Chapter Data RDD.
 * IF test mode is set to true, then we hardcode in memory list of values. They will match exactly what we have on the captions and in PDF practiclas guide.
 * 
 * When we finish building our script we can switch test mode to false and THEN it will read in a real file `chapters.csv`. These CSV files are under resources, viewing figures. In the chapters csv
 * we dont have the header names since it would be annoying to process the header and then get rid of it. HEader file will often be put into a separate file so it doesnt get in the way.
 * 
 * A cool thing you can do is when you are reading the text file, you can use the wild card `*` to load all the csv files. Ex: /viewing figures/views-*.csv THis will load in views-1, views-2,
 * and views-3 so it will load 3 csvs automatically at once. THis combines them all into one RDD.
 * 
 * Okay lets do the warmup exercise which is to build an RDD containing a key of course IDS together with num of chpaters on that course.
 * 
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		sc.close();
	
	}
	
}
