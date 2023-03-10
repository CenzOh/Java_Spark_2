//Lesson 34
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

public class Lesson34 {
	
	public static void main(String[] args) {

	
		//Lesson 34 Left Outer Joins and Optionals
/* Inner joins are def the easiest types of joins to work with. Often the join we need. Can imagine we have a big data file. Maybe its dirty data (invalid values) such as userid 10 could be
 * long since deleted and we dont want to deal with it. Inner join will discard that value from resulting RDD is quite useful. Also user id 1, john has never visited the site so we dont want
 * that data and inner join will exclude that too. If instructor recalls, working with database tables most common type of join is actually an outer join because when doing DB tables we are
 * doing some kind of report. On that report we want to see that john exists but never visited. Of course, depends on requirement and we may find our first choice of join would be the inner join
 * 
 * Lets look at a (left) outer join. They are more complicated to work with as we will see. Lets explain how it works: The left is talking about which direction we are going
 * when we do the join since it is directional. We will have one RDD in our case this is teh elft RDD (user ID and visits) and then we have the right RDD (user id and name). 
 * With a left outer join we start with the left RDD and ensure all the suer ids are present in the resulting join. So we will see three values in our new rdd. FOr IDs 4 and 6
 * we will see same results as before. But for ID 10, it WILL be in resulting RDD. But what will the value be since it has no name? Lets see.
 * 
 * userId | (visits, name)
 * 4	  | (18, Doris)
 * 6	  | (4, Raquel)
 * 10	  | (8, null)
 * 
 * This is our first thought. Well its complicated. Instead we get an object called optional.empty. This is a regular Java object. 
 * 
 * userId | (visits, name)
 * 4	  | (18, Optional[Doris])
 * 6	  | (4, Optional[Raquel])
 * 10	  | (8, Optional.empty)
 * 
 * Optionals is a new Java 8 thing. THis concept is present in many modern languages like Scala. Idea of optional is this is a value that MIGHT not be present. We were always able to do that
 * with NULLs in Java but recall that NULLs can be problematic because we may cause a null pointer exception if we try something with this value! Optionals are wrappers for objects and in the
 * case of use ID 4, we will get back the optional object. Inside the object we want, Dorice and the user ID 10 we WILL get an object back. We can query it and ask if the object has a value
 * or not and it certainly does complicate the types.
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
		
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users); //changed to left outer join
/* making the call is easy, just use leftOuterJoin. Types are similar before except our names are Optional and we wrap it with string. This is where Java is much uglier than other languages
 * dont work out from scratch, just do the quick fix to make the local var
 * OUTPUT:
 * (6,(4,Optional[Raquel]))
 * (4,(18,Optional[Doris]))
 * (10,(9,Optional.empty))
 * 
 * Interesting results, expected results and 10 is not populated. What if we want to print every name in upper case instead of printing whole tuple?
 * 
 * joinedRdd.foreach( it -> it.toString()); //cant do this because it might NOT be a string. Each element in RDD is a tuple
 * 
 * What we need to do is extract second element within the tuple and because it is nested, extract the second element from THAT tuple
 */
		joinedRdd.foreach( it -> System.out.println(it._2._2)); //first _2 is second value from outer tuple and second _2 is second value from inner tuple
/* OUTPUT:
 * Optional[Raquel]
 * Optional.empty
 * Optional[Doris]
 * 
 * Great it works! Requirement is to convert them to uppercase. Recall we DONT have a strin ghere we have an optional. Have to use get method which extracts value
 */
//		joinedRdd.foreach( it -> System.out.println(it._2._2.get().toUpperCase())); //should work but we get an error because we hit null pointer exception. We do get first two names tho
/* What is point of this tehn? IDea of optional is we can get clever HOW we work with the optional and end up with impressive readbale code. There are better alternatives. 
 * We can call the method is present and if there is a value present, we can go ahead and do something with it which is ok, still tedious.
 */
		joinedRdd.foreach( it -> System.out.println(it._2._2.isPresent()));
/* When we have an optional we can call the method or else. It will return the value thats contained inside the optional IF it is present! However, if there is nothing inside there
 * we can specify in here what we would like to be returned instead. We can return blank for instance. We are always working with objects.
 */
		joinedRdd.foreach( it -> System.out.println(it._2._2.orElse("blank") )); //def now when we get to here we will have a string of objects that we can call uppercase on.
/* OUTPUT:
 * DORIS
 * RAQUEL
 * BLANK
 */
	
		sc.close();
	
	}
	
}
