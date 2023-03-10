//Lesson 13
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2; //this is the class we are importing below, in the scala package

public class Lesson13 {

	public static void main(String[] args) {

		List<Integer> inputData = new ArrayList<>();
		inputData.add(35); 
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> originalInteger = sc.parallelize(inputData);
		
//		JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalInteger.map( value -> new IntegerWithSquareRoot(value) );
//		JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalInteger.map( value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)) ); //using Tuple2 instead of having to make a new class
		JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalInteger.map( value -> new Tuple2<>(value, Math.sqrt(value)) ); //recall if we declare type on left side, can remove on right side and infer

/* Lesson 13 Concept of Scala tuples. Tuple means collection of values. Ex - in scala, (64, 23.543), this is a tuple. Can be any type for instance - ("Lamb", "Goose", "Partridge")
 * can have as many items inside as we like. Can have objects as well. Tuples have neat syntax. Val items = ("Lamd",... val means declare new value. So this is not different than a collection
 * like a list. Difference is that a tuple is a small collection of values we will not be modifying. THink storing coordinates, instead of making a class we can use round brackets to pass them in.
 */
		
		//myValue = (9,3); //this would be great if we can do this. THis is how we would write this in Scala. This is syntatic sugar (simplified way of writing something)
		Tuple2<Integer, Double> myValue = new Tuple2<> (9, 3.0); //more traditional format and what the above would convert it to. We can import the tupl2 class. Not part of Java 
/* we can use this library in our code. THis will compile (make sure to add the Tuple2 at the beginning of myValue so we can tell Java what kind of object this is)
 * point of this is, instead of using int with sqrt and rather than invent a new class, we can use straight out of the box a tupl2.
 * if we want to use a tuple containing five elements, we can use tuple5. 
 */
	
		sc.close();
	}

}
