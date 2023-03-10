//Lesson 16
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2; 

public class Lesson16 {

	public static void main(String[] args) {
		
// Lesson 16 Coding a ReduceByKey
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
		
		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair( rawValue -> {
			String[] columns = rawValue.split(":");
			String level = columns[0]; //not interested in the date so we can remove
			
			return new Tuple2<>(level, 1L); //instead of date we will return 1
		}); 
		
		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey( (value1, value2) -> value1 + value2);
/*there are a bunch of methods we can do based on the key such as aggregateByKey. Lots of methods end with `by key`
 * One of the useful methods is group by key. WARNING that it is easy to understand but can lead to performance issues on real world data, can cause crashes on cluster
 * what will happen with this is, the new RDD will have one entry for each key and for each key, the value will be a collection WARN [Tuesday 4 September 0405, Tuesday...]
 * once we have the new pair RDD, go through they keys and count up the values. Another reason not liking this is we will end up a PairRDD with types <String, Iterable<String>>
 * kinda awkward to work with, not a java collection like a list. A list would be easier of course. Instead we use the Iterable interface. Not easily able to call .size()
 * a better idea is to use Reduce By Key. Recall this crunches the whole RDD down to one value With pair RDD we can do something similar but richer.
 * Key WARN, Value 1. We used the trick to do a reduction to get a count of values, we can do something similar in a pair RDD with the method, reduce By Key. 
 * pairRdd.reduceByKey(value1, value2 -> value1 + value2) so the WARNs are gathered together and ERRORS gathered together. Keys gathered together BEFORE reduction applied
 * Key ERROR Value 2. This will perform better than doing a grouping.
 */
		
		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
/*with tuple, we can call the fields (not method) for type string and long. _1 and _2
 * OUTPUT:
 * ERROR has 2 instances
 * FATAL has 1 instances
 * WARN has 2 instances
 */
	
		sc.close();
	}

}
