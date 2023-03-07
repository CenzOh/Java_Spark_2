//Lesson 22
package com.virtualpairprogrammers;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 

public class Lesson22 {

	public static void main(String[] args) {
		
//Lesson 22 Practical Requirements and Keyword ranking
/* this is where everything comes together and we will make something more like a real spark program. What we want to do is: take subtitles file out of any of the courses
 * and generate automatically a set of ten keywords for that course. Key words should reflect major topics in the course.
 * If you check out spark documentaion, go to the RDD guide that will be helpful and applicable to us currently. Important ones to look at are transformations and actions
 * recall, transformations are where we tell Spark we want to transform an RDD from one form to another and spark will lazily evaluate the transformation once we get to an action 
 * take() is a good fcn for us to use (doesnt print everything since working with big data will be too much for the console). takeSample() another one, this returns an array
 * and we specify how many elements, will specify ten random elements from the RDD. Get used to using documentation!! 
 * 
 * Exercize is to do a word count, count each unique word in the file. However, we dont want to deal with boring words like course, to, the. Will appear frequently and not a specific key word 
 * we have a file called boringwords.txt. In the file we have a list of very common words in english language. We used spark to run through all the courses and generated the
 * list of words that appear frequently throughout the course. Instructor uses common words like tedious and terribly. These are words we should be filtering out of our results.
 * Dont load the file into memory, has been done for us in `Util.java` its a wrapper around the file so we dont have to load it into memory. 
 * why not load in boringwords as an RDD? it can complicate the exercise and in real life, this boring words is not really enough. Its never going to be as big as something
 * like 1 - 2 million words. No point in distributing it across an RDD. This file will be loaded into drivers JVM. We can leave it as local text file. 
 */
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		initialRdd
			.flatMap( value -> Arrays.asList(value.split(" ")).iterator())
			.foreach(System.out::println);
				
		sc.close();
	}

}
