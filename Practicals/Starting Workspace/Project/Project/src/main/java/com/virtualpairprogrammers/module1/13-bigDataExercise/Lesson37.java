//Lesson 37
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Lesson37 {
	
	public static void main(String[] args) {

	
		//Lesson 37 Introducing the Requirements
/* We will be building a practical example of using Spark that would be like what we would use Spark for in real life! THis exercise is based on a routine the instructor does.
 * Good chance to practice everything that we learned.
 * So, we need to get a feel for which courses are popular and which are not. We use that data for things like calculating royalty payments. The most important raw data 
 * is very simple, just two columns. We will call it viewData. It is a log of viewings. So at some point in time, user ID number 14 has watched a chapter on a course. In this case
 * It is chapter ID 96. Every chapter in the system has a unique identifier. The data is clean so no worries. A bit later on, same user watched chapter ID 97. Later on a different user this time,
 * user ID number 13 also watched chapter 96. But then (this is important) the same user watches the same chapter again and again for a total of three times (three entries). Note that
 * we do NOT want to count those three viewings as separate viewings, we just want to count that this user watched chapter 96 ONCE.
 * 
 * viewData
 * userId | chapterId
 * 14	  | 96
 * 14	  | 97
 * 13	  | 96
 * 13	  | 96
 * 13	  | 96
 * 
 * End result of the exercise (this is a long exercise) are to get a sense of which courses are popular and which are not based on this raw data. The issue with the exercise is that
 * we can not reference the courses. Could be that the different chapters are different courses. Or maybe all the chapters are a part of the same course. Or maybe chapters 96 and 97
 * are one course and chapters 99 and 100 are on a different course! THis is why we have a separate database dump! Its a smaller dump and a mapping of chapter IDs
 * 
 * chapterData
 * chapterId | courseId
 * 96		 | 1
 * 97		 | 1
 * 98		 | 1
 * 99		 | 2
 * 100		 | 3
 * 100-109	 | 4
 * 
 * We know now that chapters 96-98 are for one course and chapter 99 is for course 2, the only chapter on that course. 101 through 109 is part of course 3, much bigger course
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4, 18));
		visitsRaw.add(new Tuple2<>(6, 4));
		visitsRaw.add(new Tuple2<>(10, 9));
		
		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<> (1, "John"));
		usersRaw.add(new Tuple2<> (2, "Bob"));
		usersRaw.add(new Tuple2<> (3, "Alan"));
		usersRaw.add(new Tuple2<> (4, "Doris"));
		usersRaw.add(new Tuple2<> (5, "Marybelle"));
		usersRaw.add(new Tuple2<> (6, "Raquel"));
		
		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
	
		sc.close();
	
	}
	
}
