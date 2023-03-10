//Lesson 33
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Lesson33 {
	
	public static void main(String[] args) {

	
		//Lesson 33 Inner joins
/* We will be looking at how to perform joins on RDDs. WE can do similar to how we do this with relational database tables. Lets say we have an RDD with massive data and its pair RDD.
 * Keys are UserId (this is info for a website) we map against user ID how many visits each user made to the site. Ex - Userid 4, visits 18.
 * In separate RDD we have another pair RDD. Keys are user IDs and the values are names of users. Ex - Userid 4, name Doris. Now notice that not ALL the user IDs are present in both
 * tables. Ex, user 10 is NOT shown in the right table but ONLY in the left table. And users 1,2,3,5 are in the right table not in the left one. ALSO ITS NOT TABLES ITS RDDS.
 * What we want is for end result to list user name together with num of visits. We have some options.
 * 
 * Easiest option, inenr join. In the API this is just called a Join, so think of this as a regular join. What happens with this join is Spark will combine the two RDDs to get a new RDD and
 * whenever a user ID appears in BOTH tables, will combine the values together. SO that means we will get user IDs 4 and 6. Key will be user Id. Value will be left hand RDD together
 * with value of right hand RDD. For any elements that DONT appear in both tables, no entry in new RDD. Such as user id 10, no entry for this in new RDD.
 * 
 * New RDD:
 * userId | (visits, name)
 * 4	  | (18, DOris)
 * 6	  | (4. Raquel)
 * 
 * Values are tuples! Rememebr tuples from before? This can get a bit complex in the code because the RDDs in themselves are tuples. Its a tuple of user ID with a value so we are essentially
 * nesting tuples. We will test the joins below:
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
		
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
/* no inner join method its just called join(). THis generates a new RDD remember. THink of what parameters will be in the javapairRDD.
 * I though, <Integer, Tuple2<Integer, String>> I was right. Lazy way is when you write joinedRDD select create local variable option and compiler will do it for us.
 *  May get strange ordering because this is running multi threaded
 *  OUTPUT:
 *  (6,(4,Raquel)
 *  (4,(18,Doris)
 */
	
		sc.close();
	
	}
	
}
