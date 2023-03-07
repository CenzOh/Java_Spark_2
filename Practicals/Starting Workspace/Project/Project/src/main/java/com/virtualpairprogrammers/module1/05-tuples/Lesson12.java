//Lesson 12
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.virtualpairprogrammers.IntegerWithSquareRoot;

public class Lesson12 {

	public static void main(String[] args) {

		List<Integer> inputData = new ArrayList<>(); //arraylist can assume it will be integer as we defined first where the List is
		
		inputData.add(35); 
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
/* Lesson 12 Tuples. THis is not spark specific, the concept is originally from scala. Example of tuple - lets say we have the requirement to store these integers in the RDD
 * but we need to store their corresponding sqrt (need both sets of data)
 */
		
		JavaRDD<Integer> originalInteger = sc.parallelize(inputData); //renamed this to original integer
		JavaRDD<Double> sqrtRdd = originalInteger.map( value -> Math.sqrt(value) );
	
/* Nothing wrong with this but with more complex operations, we will want to keep the data together in a coherent record. 
 * Keeping the data in two seperate RDDs will be hard to do anything useful. Goal - keep the integer and corresponding sqrt in same row
 * ATM - RDD 35. RDD 5.916. Point is, we have TWO separate RDDs. Lets put them together in ONE RDD as such: RDD 35 | 5.91608. This RDD will have two columns. 
 * May feel like a Java Map or relational DB table. Note that an RDD can contian any type of data including Java Objects. One thing we can do is create an instance:
 */
		
//		IntegerWithSquareRoot iws = new IntegerWithSquareRoot(9); //dont need to input the three we can caluclate inside
	
//What we can also do is change our sqrtRdd to say given a value which will be a raw int, we can return a new instance of our integer with the square root passing in the value
		JavaRDD<IntegerWithSquareRoot> sqrtRdd1 = originalInteger.map( value -> new IntegerWithSquareRoot(value) ); //in lambda syntax we dont need return keyword and type is Int with sqrt
		
//general pattern is NOT to do this and instead use the scala concept tuples. Will discuss next lesson.
		
		sc.close();
	}

}
