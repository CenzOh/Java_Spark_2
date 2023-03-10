//Lesson 18
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2; 

public class Lesson18 {

	public static void main(String[] args) {
		
// Lesson 18 Group By Key
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

		//groupbykey, again this can be problematic later on but we will temporarily show how this can work
		sc.parallelize(inputData)
			.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
			.groupByKey()
			.foreach( tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances") ); //will be key value pair, we want to know the size of it
		
		//iterables. we can see guava libraries in our clas path since they are dependencies of spark
		
//gives us a key with the values in a list. Iterable does not ahve a size method on it. Again, not really intuitive, output will be same as before. Use reduce by key method instead!!!
		
		sc.close();
	}

}
