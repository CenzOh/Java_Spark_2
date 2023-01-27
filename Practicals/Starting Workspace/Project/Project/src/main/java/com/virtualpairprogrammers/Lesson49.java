//Lesson 49
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Lesson49 {
	
	public static void main(String[] args) {

	
		//Lesson 49 the DAG and Spark UI
/* Previous lesson's concept will be relevant to EVERY perofmrance aspect we will be looking at throughout the rest of the chapter. The difficult thing about spark is, it almost feels
 * like magic what it does under the hood. Very complicated, hard to reason what is going on at runtime. Instead of looking at program and guessing what is happeneing, it is 
 * recommended to look at the web user interface to get a FULL understanding of that execution plan otherwise known as the DAG. Stands for Directed Acyclic Graph
 * its jargon from computer science. This means it is a graph that does not have ANY loops inside it. Recall when we looked at elastic map reduce. We saw on EMR that we can look
 * at the history server which was running on localhost:18080. The history server will gather together all previous jobs we already ran. It is supplied by a script
 * which was running on our EMR server and that was pre configured by Amazon so we didnt have to start that script. When we downloaded apache spark, there is an option
 * we have part of that distribution of scripts we can run which will run a locals web server and give us access to the history server. No need to install for the course but worth looking at.
 * 
 * We have a little hack, not elegant but works for basic testing. When our program is running, spark will start a web server on port 4040. This will show us the progress of our
 * current executing job. We still have the issue that we will only have access to this while the job is running / java program is running. So a hack is, before the sc.close()
 * we can hold the console:
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
		
		Scanner scanner = new Scanner(System.in); //scanner obj from java.util this allows us to read from the console
		scanner.nextLine(); //this will make the console wait for a text input so we can visit the web ui
		
/* Downside? Easy to forget to terminate the program. We will notice we have done that and we receive a warning and the next program says can not bind port 4040.
 * IF you run this program not with debugger, we have results you can see the red button is illuminated. It is waiting for us to enter something. Localhost:4040 gives us similar page to
 * the history page. What we have here is a list of completed jobs. A job represents any action we have performed in our script. Roughly true because we performed ONE action in our script
 * which was the take() method. However, this page also sortByKet() is considered a job. This is due to internal implementation of sort by key which generates a job even though sort by key
 * is a transformation NOT an action
 * 
 * If you click on the sortByKey() job we can see the execution plan. Spark calls this the DAG. This is what we have been building up in our code. Each transformation is added to the DAG
 * Once we understand what the stages are, then we will see that the transformations are being carried out one after the other. So for instance we can see: Stage 0, textFile -> map ->
 * filter -> flatMap -> filter -> filter -> map -> Stage 1, reduceByKey -> map -> sortByKey
 */

		sc.close();
	
	}
	
}
