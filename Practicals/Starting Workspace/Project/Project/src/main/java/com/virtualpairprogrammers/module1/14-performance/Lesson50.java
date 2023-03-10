//Lesson 50
package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Lesson50 {
	
	public static void main(String[] args) {

	
		//Lesson 50 Narrow vs Wide Transformations
/* Lets run through simple example we will code up as well to show resulting execution plan over on real spark. Imagine we have a text file, this is terabytes from some server and
 * is logging like we saw before. We have logging level, followed by a colon, followed by a date and time. Ex:
 * WARN: Tuesday 4 September 0405
 * On the execution plan, we will call the text file method which adds a node into the execution plan. What it really does is tell the worker node to load fragments of text into memory
 * and we are going to end up with a series of partitions (introduced in opening chapter). As a reminder, a partition is a chunk of data. Now imagine on our deployment, we are going to have
 * worker nodes like when we ran elastic map reduce while thinking about it for a tera byte file. We need MORE than two worker nodes for this since we need to fit each
 * partition into ram. Assume that two worker nodes are sufficient for our example. Note that we will typically see multiple partitions on each of the nodes.
 * 
 * Why do we see multi partitions? Why not just one partition per node? How does spark determine this partitioning? Answer is it depends on input source for the RDD. When working
 * with text file like this, then its simple case of spark chunking the data. We saw this in elastic map reduce chapter. We loaded the two gigabyte file and we saw that the partitions
 * were on elastic map reduce and they were 64 mb each. Nothing complicated, Spark just arbitrarily saying `we will have each partition contain some num of megabytes of data`.
 * 
 * Thats how we obtained the initial partitions. If this RDD had been seeded by a data source other than a text file, it would have simply used hashes / hash code of these strings
 * and it would have used a modulus of that hash code to randomly spread the strings between different partitions. Thats what we mean by it depends on input source. So now we have our strings
 * scattered sort of randomly across the partitions. Randomness is just we have taken chunks off the data but now we are going to go on and do a transformation.
 * 
 * Easy transformation example is a filter. Lets say we are only interested in warnings. We will filter out and only include the lines that start with string `WARN`.
 * filter (line -> line.startsWith("WARN"))
 * We know how this works by now, lambda expression distributed to individual nodes actually distributed to each of the partitions where it will become a task. So the block of code
 * executing against the partition is called a task. With our simplified example, we will end p with six tasks (three were in each partition) and those tasks will execute and we will
 * end up with our transformed RDDs. One of the partitions will be empty because it includes FATAL and ERROR but we are looking to filter for only WARNs.
 * 
 * All of this can be done in parallel and this fcn can be applied to each partition. Spark implements that transformation w/o having to move any data around. It didnt have
 * to change the partitions in any way at all. So for that reason, this type of transformation is called a Narrow transformation. Lets look at another. So if we go back to the original inputs
 * RDD with the six partitions and ERRORs and FATALS back, we have commonly done a mapToPair() transformation. The input data is a useless string. As we have seen on the course,
 * it is common to split it out to key value pairs. Our result would look like: (ERROR, Friday 17 July 1052). Key is the code and value is date and time. We can do rich operations on the key
 * such as grouping, sorting, etc. Important thing, this is ALSO a narrow transformation. No need for Spark to move the data around. Each input partition becomes an output partition.
 * 
 * So note that when we have one of these narrow transformations, we will end up with the situation where the keys are scattered across the partitions. Here is another transformation,
 * groupByKey(). This creates a new pair RDD where for each key we have a list of all the dates that logging level occurred. Only way Spark can implement this is, for instance, it gathers
 * together all the fatals. To do this, it will have to move the data around from one partition to another. This is because the partition is spread across multiple physical nodes!
 * This involves network traffic, converting these values which is okay for us because they are strings but it could be any Java object~ Convert the objects to binary so it can transfer
 * across network. IE - it has to serialize all data BEFORE moving it across nodes. Painful and expensive operation. This is a Wide transformation. Spark must shuffle al
 * strings around to get them so that they are all clumped together. SPark does call this process shuffling. Shuffling is triggered through any wide transformation.
 * So we end up with a single key of WARN with a list of all dates. Same with other keys. Note that we end up with three partitions but there are still three partitions that are empty.
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
		
		Scanner scanner = new Scanner(System.in); //scanner obj from java.util this allows us to read from the console
		scanner.nextLine(); //this will make the console wait for a text input so we can visit the web ui
		

		sc.close();
	
	}
	
}
