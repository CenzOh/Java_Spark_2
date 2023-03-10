//Lesson 48
package com.virtualpairprogrammers;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Lesson48 {
	
	public static void main(String[] args) {

	
		//Lesson 48 Transformations and Actions
/* We will now be looking at performance. We ignored before because we needed to tackle the fundamentals first. We do not need to know everything that is 
 * going on inside Spark but having a good handle on the underlying model of Spark gives us a better chance of writing good Spark Jobs.
 * First we will look at difference between transformations and actions. We have lightly touched this earlier but we will drill down on it now.
 * We will then look at the DAG / execution plan and use the Web UI back in chapter with Amazon Elastic Map Reduce. 
 * Then we will return to transformations and explain difference between Narrow and Wide Transformations. Also looking at shuffles and stages that appear on the console.
 * We will look at partitioning and one possible issues called a Key Skew.
 * We will look at the group by key transformation, which we have looked at earlier in the course. Some issues with it, not performance related but operation issues. Easy to get
 * an out of memory exception in our cluster when working with group by key. General advice - avoid group by key, always to do things in a different way.
 * Will look at caching and persistence. If we understand transformations and actions and can read DAG, this concept will be easy. Only a single line of code.
 * 
 * We are returning back to the Main program we wrote when we first did the keyword ranking. I will include it below. What we are doing is reading an input text file and generate
 * a set of keywords ranked by popularity. In the intro chapter we talked about on each line of code, think like this we take an RDD, filter it, end up with new RDD.
 * Maybe we think theres a great big set of data for the RDD and each line of code we modify the RDD. THis is not entirely true at runtime. We are NOT building RDDs on each step.
 * Instead, we are building an execution plan. Only when we get to an operation is where spark does the calculation to provide the results. Only point Spark needs to do calculations 
 * is the one with the take() method (in our case). The reason is we are asking for a Java result. Notice left hand side is a regular list than an RDD. Again ONLY that line of code is
 * when Spark commits to doing calculations. Each previous line builds an execution plan. Spark makes differentiation of operations; this operation here is called an action (method)
 * 
 *  Actions make calculations happen and they are generally going to result in some regular Java objects. Think of actions as `get me the results`. The previous operations (which are NOT
 *  building RDDs) are called Transformations. These add new steps to the execution plan. Must understand difference. May not be obvious why difference is relevant. We will see later on.
 *  Giving a quick demo in debugger, double click left hand column. Put breakpoints so we can see what happens with the initialRdd line.
 *  
 *  So we are on line 28 looking at debug menu, have not executed it yet. Until now, we would have been commenting saying we read in text file. What we actually do is NOT loading
 *  in text file, we are telling spark to include in its execution plan the loading of the text file. What this means is, the text file will ONLY be read in when it needs
 *  to run the execution plan. Which occurs on that .take() method line. If we do a step over we can see each transformation doesnt write anything in the logger.
 *  Again we are building the execution plan. When we get past .take() which is reading into the results object, you can click on results in the variables section of debugger and 
 *  you will see that results is a normal java object. Now at this point, Spark has to do the calculations. Refer to spark documentation to see which methods are transformations
 *  and actions. We can see a lot of transformations we used like distinct(), map(), flatmap() are all transformations. We also say they are lazily executed. THis means jobs only have 
 *  to be done when we reach the action. COmmon actions are collect(), count(), first(), take(), saveAsTextFile().
 * 
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//all from lesson 26
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");	
		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll(" ^[a-zA-Z\\s] ", "").toLowerCase()); 
		
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0); 
	
		JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
	
		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair( word -> new Tuple2<String, Long>(word, 1L));
	
		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));

		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		

		sc.close();
	
	}
	
}
