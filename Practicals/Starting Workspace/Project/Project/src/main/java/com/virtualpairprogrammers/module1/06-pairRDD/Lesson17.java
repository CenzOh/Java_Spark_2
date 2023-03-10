//Lesson 17
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2; 

public class Lesson17 {

	public static void main(String[] args) {
		
// Lesson 17 Using Fluent API
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
/* There is a concern about the code. Its a bit messy. Needed about 10 lines of code. We can use Java 8 lambdas to make teh code look similar to Scala or Python
 * We can refactor the code to a single line of executable code. Its because all the methods we call on the RDD are fluent - given the operations, end result will return an
 * instance of the RDD we are working on which is the return value. Once we have done something. We will see all fcn s we need. Dont really store the results in an 
 * intermediate variable. We can also optimize the lambda fcn and collapse into shorter lines of code. We do not need to have our operations spread across four separate lines
 * of java with intermediate variables. lets do it in one line.
 */
		
		sc.parallelize(inputData)
			.mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L) )
			.reduceByKey( (value1, value2) -> value1 + value2)
			.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
//This code code here is identical in operation to the code we had before. This is the normal way of writing a Scala program. Dont need to copy it since its a spark idiom so we can use the fluence api
		
//		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair( rawValue -> new Tuple2<>(rawValue.split(":"[0], 1L)); //first change, exactly what we had before expressed as one line
//		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey( (value1, value2) -> value1 + value2); //removed
//		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances")); //removed

		sc.close();
	}

}
