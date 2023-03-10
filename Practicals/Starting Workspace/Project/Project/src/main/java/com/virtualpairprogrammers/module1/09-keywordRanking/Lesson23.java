//Lesson 23
package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 

public class Lesson23 {

	public static void main(String[] args) {
		
//Lesson 23 Working Solution
/* We will be reusing the main class we have been working on. Load in initial RDD which is input data. Big data will be partitioned and split across multiple nodes in a cluster.
 * The initial data is actually very noisy. Let us use the take() method to look at a smaller subset of the data.
 */
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
/* Output for Take method for the initialRdd:
 * 1
 * 00:00:00,286 --> 00:00:01,777
 * - [Instructor] Hello and welcome to this
 */
/* We can see a lot of irrelevant information such as the timestamps and sequential number. Blank lines also have no relevance. How can we get rid of this redundant data?
 * An easy way is to run a map, for every line replace anything that is not a letter with a blank. We wills trip out the square brackets, the whole line with the time stamp.
 * Almost regular java! Use the replaceAll method. First part of this method we have to use a regular expression (regex). We will also make a new var for every transformation then we will compact down later
 */
//		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll(" ^[a-zA-Z] ", "")); //this says antyhing that is NOT in range of a-z upper or lowercase, replace with a blank
//after running take method on letters only RDD, it failed. It compacted every sentence into single words. Lets fix this. Everything thats not a character must be stripped out.
		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll(" ^[a-zA-Z\\s] ", "").toLowerCase()); 
//all we did was add a space at the end and it works. Use \s (for java use two \ to rid error)
		
/* Something else we can do at same time, we have some words with capital letters like Docker and Java. Lets make it easier for ourselves by lowercasing those words.
 *  Simply call the fcn .toLowerCase() at the end of this operation. Just a simple java operation.
 *  Next thing to get rid of are the blank lines. Obv thing is to run a filter and filter out any line that is a blank
 */
		
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0); 
/*maybe this will work. Some blank lines are still here. Its because the lines have some spaces or tabs. So lets add the fcn .trim() which removes unprintable characters and whitespace!!
 * Next lets get this data into the format of words. We will be using a map which will split each line into its constituent words. We know there will be one or more words for each line. Use flatmap
 */	
		JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator()); //recall from previous chapter about this
/* Here we want to split on a space, convert it to a list using arrays as list. Spark doesnt know what a collection is so we call iterator to make Spark happy.
 * Now lets test if it works. And it does!!
 * OUTPUT:
 * instructor
 * hello
 * and
 * ...
 * WE HAVE A PROBLEM - most of these words are BORING. Now let us filter out the boring words
 */
		JavaRDD<String> justInterestingWords = justWords.filter( word -> Util.isNotBoring(word));
//recall we have the Util java class with the helper function .isNotBoring() so call that within the filter method. 
/* OUTPUT:
 * ansible
 * jenkins
 * docker
 * ...
 * Def more interesting. We did notice we had a blank line in the output sadly. What may be happening is after filtering out blank lines, a few sentences may have double spaces. 
 * Maybe we can reorder them so that the flatmap split comes before the filter trim. Maybe we will cover next lecture
 */
		
//		List<String> results = initialRdd.take(50); //take first how many we specify 
//		List<String> results = lettersOnlyRdd.take(50);
//		List<String> results = removedBlankLines.take(50);
//		List<String> results = justWords.take(50);
		List<String> results = justInterestingWords.take(50);
		results.forEach(System.out::println); //standard java, below is some output
		
//		initialRdd
//			.flatMap( value -> Arrays.asList(value.split(" ")).iterator())
//			.foreach(System.out::println);
				
		sc.close();
	}

}
