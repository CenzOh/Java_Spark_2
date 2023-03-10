//Lesson 35
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Lesson35 {
	
	public static void main(String[] args) {

	
		//Lesson 35 Right Outer Joins
/* A right outer join is exactly the same thing! But we are going to be starting from the right RDD (userid and name) this time! We know in the resulting RDD there will be a value for all the 
 * user IDs in the right RDD which are IDs 1-6. We will end up with six values. Same idea, if there is an ID that is rpesent in right RDD but not present in left RDD (like ID 1), we will
 * get one of those optional dot empties again.
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
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users); //changed to right outer join make sure to change types!!
/* OUTPUT:
 * userid | (visits, name)
 * 1	  | (empty, John)
 * 2	  | (empty, Bob)
 * 3	  | (empty, Alan)
 * 4	  | (18, Doris)
 * 5	  | (empty, Marybelle)
 * 6	  | (4, Raquel)
 * 
 * This is our output, we removed optional because we will take those as read. THis would be great use of our or else
 * `it` is for every row. We want to get the users name which is in teh second value (the tuple, NOT the ID) second value of that (the name, NOT the num of visits)
 */
//		joinedRdd.foreach( it -> System.out.println("user" + it._2._2)); //does get the usernames 
		joinedRdd.foreach( it -> System.out.println("user" + it._2._2 + " had " + it._2._1.orElse(0) + " visits.")); 
		
/* Okay again, it._2 this gets the tuple, _1 gets the num of visits NOT the name. THe orElse will be returned if we meet an empty optional (remember thats the type we learned).
 *  OUTPUT:
 *  user Raquel had 4 visits
 *  user John had 0 visits
 *  ...
 */


	
		sc.close();
	
	}
	
}
