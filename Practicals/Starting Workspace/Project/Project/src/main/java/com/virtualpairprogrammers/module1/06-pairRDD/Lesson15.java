//Lesson 15
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD; //new import
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2; 

public class Lesson15 {

	//@SurpressWarnings("resource") //this removes the warning we are seeing at the return statement, dont need but can help remove the warnings
	public static void main(String[] args) {
		
// Lesson 15 Building a pair RDD
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData); //raw RDD
		
//usually set up initial RDD with raw data, convert to data structures we need. Recall spark is clever to organize jobs in an efficinet execution plan
		
		JavaPairRDD<String, String> pairRdd = originalLogMessages.mapToPair( rawValue -> {
			String[] columns = rawValue.split(":"); //simple split by the colon. Will return an array of strings
			String level = columns[0]; //the key for this pair RDD
			String date = columns[1]; //the value. Key value pair
			
			return new Tuple2<>(level, date); //key value pair RDDs work with tuple2. Can remove the <String, String> since it will be inferred from the JavaPairRDD
//getting a warning here, thinks there may be a data leak with the lambda but we should be okay since we close outside the fcn
		}); //transform into type of structure we want which is a new pair RDD, use the map to pair method
		
//split the values onto different lines. Spanning multiple lines is common. Do this with curly bracket {} can do several lines of code in lambda fcn. Split by log level and date and time
//JavaPairRDD is our new type, make sure to import and to specify the type of both elements for JavaPairRDD.

	
		sc.close();
	}

}
