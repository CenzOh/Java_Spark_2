//Lesson 39
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson39 {
	
	public static void main(String[] args) {

	
		//Lesson 39 Main Exercise Requirements
/* Main Exercise - Produce a ranking chart of the most popular courses, using our `Weighted COurse View Ration Process`
 * courseId | score
 * 1		| 6
 * 2		| 10
 * 3		| 0
 * 
 * Each course gets a score. These are the score values we should end up with. Okay so how do we work them out? The algorithm is this:
 * If a user watches MORE than 90% of the course, course gets 10 points.
 * If a user watches > 50% but <90% it scores 4.
 * If a user watches > 25% but <50% it scores 2.
 * Otherwise, no score
 * 
 * REasoning behind this weighing: if someone only watched a quarter of the course, they didnt really get anything out of the course. No value to them so it deserves a 0.
 * Now this is just for a single user. Each course will be watched by multiple users. So we expect these scores to accumulate which is why one of the courses scored a six. We can guess that
 * someone watched more than half of the course and another person watched a bit less than half of that course. Again remember the scores are for a SINGLE users viewing of a particular course.
 * Lets put it another way: lets say we have an example where a course has 10 chapters and one user watches all 10 chapters. The score then is a 10. However, the converse is NOT true if
 * 10 users come along and watch just ONE chapter of the course. In that case we would give it a score of 0 because for each user they only watched a tiny percentage.
 * 
 * Course with ten chapters
 * One user watches all 10 chapters => Score = 10
 * 10 users watch one chapter => score = 0
 * This means as we work through the exercise we need to make sure we keep hold of WHO watched EACH chapter. Keep the views separated.
 * 
 * One concept we have not encountered yet until now. If we look at the test data we can see that the user ID 13 watched chapter 96 three times. THis doesnt count as three views, counts as
 * ONE chapter viewing. We need to get rid of duplicated rows in the RDD> Very simple method called .distinct(). WIll implement in the viewingFigures.java file
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//Here I will write MY implementation / on my own BEFORE going over solution
//Step 1 - remove duplicated data - given to us
//		viewData = viewData.distinct();
//		viewData.foreach(System.out::println);
		
//Step 2 - join to get COurse ID in RDD
//		JavapairRDD<Tuple2<Integer, Integer>> newRdd = viewData.map( row -> new Tuple2<>(row._2,row._1)); //something like this to swap chapter ID and user ID
//		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = newRdd.join(chapterData); //something like this to join course IDs with chapter and user IDs
		
//step 3 - drop course ID and count each occurrence
//		JavaPairRDD joinedRdd<Tuple2<Integer, Integer>(level, 1L); //we have to do something to change the chapterID column to litterally just be value 1 for count purpose
		
//step 4 - count views for user / course
//		JavaPairRDD<String, Long> sumsRdd = pairRdd = pairRdd.reduceByKey( (value1, value2) -> value1 + value2); //then we have to add the counts to get sum with reduce
		
//step 5 - drop userID 
//		JavaPairRDD<Integer, Integer> newViewsRdd = sumsRdd.map(); //then we do another thing to map and say just have course ID and views
		
//step 6 - of how many chapters
//		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterCountRdd = chapterData.sum(); //grab total num of chapters we want views of. I believe get chap count with sum method
//		JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.maptoPair( row -> new Tuple2<Integer, Integer>(row._2, 1)) //this was given to us I believe this is it
//				.reduceByKey( (value1, value2) -> value1 + value2);
		
//step 7 - convert to percentages.
//		JavaPairRDD<Integer, Double> percentageRdd; //another thing we have to do so we just divide the views by total num of chapters
		
//step 8 - convert to scores
		//I believe we would have to do something with comparing percentage with value.Ex - 0.6 would be >50% but < 90% so that should turn into a 4. Think we would
		//have to create our own fcn or ruleset for this
		
//step 9 - add up total scores
		//just doing .sum() on these.

		

	
		sc.close();
	
	}
	
}
