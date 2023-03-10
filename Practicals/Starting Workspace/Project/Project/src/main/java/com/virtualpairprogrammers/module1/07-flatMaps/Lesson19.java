//Lesson 19
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 

public class Lesson19 {

	public static void main(String[] args) {
		
// Lesson 19 FlatMaps and Filters
/* map transformation should make sense now. Given an RDD, we run a map, take a fcn, apply it to every value in RDD. we get new RDD based on results of mapping fcn
 * function = sqrt(value) RDD 35 -- sqrt(value) --> RDD = 5.91608
 * There is a restriction with map. There must be ONE output value for every single input value. Some circumstances this wont be the case. We may need multiple outputs or no outputs.
 * This is where flat maps come in. THink an input RDD has a series of string which are sentences. First sentence contains `the quick brown fox` and the second sentence is a gap, an
 * empty string. Third sentence contains `jumped over blah`. Common requirement is to split the RDD so it contains single words.
 * A map would NOT work ehre. FOr the first line, we want to see four elements in our new transformed RDD. Would split into four new strings. Second line is blank so same fcn
 * would be applied and return nothing while the same fcn to third line should generate three strings. Again, flatmap works here.
 * Flat map does this: given a single input, 0 or more output values will be the result. Called this because once spark did the map, it will have multi values and will flatten.
 * in spark, this will create an RDD with the values in just a single flat collection exactly as we have been seeing. So now lets split the input data into individual words
 * that appear in our log
 */
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		sc.parallelize(inputData)
//			.mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L) )
//			.reduceByKey( (value1, value2) -> value1 + value2)
//			.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

		JavaRDD<String> sentences = sc.parallelize(inputData); //first make an RDD out of our input which rn are sentences
		
//		sentences.flatMap( value -> value.split(" "));
/* apply the flatmap fcn givee a value which will be the individual elements of the RDD. we want to split this up into individual strings.
 * the above will initially wont compile since the contract of the flatmap method. Look at api or doc to understand what is going on. Compiler says mismatch error
 * cant convert from string to iterator unless handled string array first, we may recall that when call split on a string it does NOT produce a java collection
 * its a raw string array. What we need is a regular Java collection NOT an array. There really isnt any method that return a collection such as a list. Common trick is to
 * use the convenience method:
 */
		JavaRDD<String> words = sentences.flatMap( value -> Arrays.asList(value.split(" ")).iterator()); //will convert into a regular list of strings. Standard java. Next compile error we see is

//the api wants us to return an object of type iterator. Every collection has a method called iterator and returns iterators. Thats what the API is looking for so just write
//.iterator() at the end
		
		words.foreach(System.out::println);
		
//Tuesday
//4
//...

		sc.close();
	}

}
