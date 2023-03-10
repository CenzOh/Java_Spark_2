//Lesson 36
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

public class Lesson36 {
	
	public static void main(String[] args) {

	
		//Lesson 36 Full Joins and Cartesians
/* Our next join to look at is Full Outer Joins. THis is a combination of left and right outer join. End result of this join is that there will be EVERY user ID that appears in BOTH of the
 * RDDs. We will have 1-6 and 10 in our final RDD> IF present, there will be num of visits, and if present, there will be the name. Potential for empties.
 * This means we will ahve to deal with optionals for BOTH parts of this nested support
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
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedRdd = visits.fullOuterJoin(users);
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd3 = visits.join(users);
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd2 = visits.cartesian(users);
/* We can do a cartesian which is like a cross join. We will end up with all the values paired up with every combo. Ex - 4,18 1,John. Another row, 4,18 2,Bob. Etc. We will end up with
 * 6 * 3 elements or 18 elements. THis is useful if we are considering matches between two sets of data. Inner join most practical. However, using left outer join can still be helpful later 
 * down the line to check if the name is empty. But again, remember you could have initially filtered them out with inner joins
 */
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>>anotherJoinedRdd = visits.rightOuterJoin(users);
/* userid | (visits, name)
 * 1	  | (empty, John)
 * 2	  | (empty, Bob)
 * 3	  | (empty, Alan)
 * 4	  | (18, Doris)
 * 5	  | (empty, Marybelle)
 * 6	  | (4, Raquel)
 * 10	  | (9, empty)
 */

//		anotherJoinedRdd.foreach( System.out.println());

	
		sc.close();
	
	}
	
}
