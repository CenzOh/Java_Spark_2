//Leson 26
package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Lesson26 {
	
	public static void main(String[] args) {

	
		//Lesson 26 why coalesce is the WRONG solution
/* Last time we used coalesce to force spark to move all our data onto ONE partition. Appeared to solve our problem. HOWEVER, it did not. Many people online will say this is the answer
 * Lets explain why! What we do with coalesce is actually creating a new problem!! Lets assume after doing this, we want to do some further operation on the side.
 * And assume that we are still in realm of big data. So by combining it all onto a single partition, it will be on a single node and single physical JVM. We may end up with an out of memory
 * exception. Will not get any further benefits from spark. This is the wrong solution because we have the wrong explanation. We need a better understanding.  
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
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
		
//We can prove this was the WRONG explanation. There is a method on our RDD called get num of partitions. Will tell us how many partitions there are. We never configured spark to use a certain amount
//		System.out.println("There are " + sorted.getNumPartitions() + " partitions"); //prints 2 partitions. The sort is also all wrong when we run this again.
		
/* We have to see a lot of data inside the megabytes range prior to seeing any partitions happening. This may be because of a min num we are working on. Tiny data is the explanation of why we only
 * have two partitions. Put a massive file into this and we'll get more partitions. The partition explanation is not right. We only have two partitions we would think we would have TWO sorted 
 * blocks but thats not the case. If explanation is right there would be hundreds of partitions.
 * 
 * Why didnt it fit / why is this wrong? When working in Spark, in some ways, we do NOT need to worry about shuffling the data or we dont even need to know about partitions for the result of
 * the actions that we are performing to be correct. So in all cases, we should be getting correct sorting REGARDLESS of whether sparks working with one partition or a thousand partitions.
 * Spark experts will not like the first statement (do not need to shuffle data or know about partitions for result to be right). Of course we do need to know but we only need to know about
 * shuffles and partitions for PERFORMANCE. Not needed for correctness. Again we'll be covering performance later.
 * 
 * Why are we seeing wrong results on console? Actually, we have simply misunderstood the contracts of for each. What happens with foreach is, driver sends for each operation to EACH partition.
 * The contracts of for each are the funcs we pass into for each are sent across to each partition in parallel and that function will execute in parallel to each partition.
 * If we deployed this to a real cluster, then each partition would be on different nodes. What would happen on each node is the print line would be executing on each node. 
 * Recall we are not on a cluster, we are on one machine. Our computer will have multiple cores and default config in Spark is for each core we have in our processor, Java will spin off a thread
 * We are experiencing two partitions in our system (instructors), there is a thread running print line on this partition with a thread running print line on ANOTHER partition.
 * 
 * The answer simply is that for each is strange and we have to be careful of the results running a foreach. Looking at output, we can look that thread one is running through partition 1 and does
 * print line. Thats why we see the numbers go monotonically downwards. Then at some point we can see lets say (1, positional) thats where we get to thread 2 print line on the second partition.
 * It only got a chance to be one output when the other thread got interrupted and thread one has started running again. Essentially, Thread 1 ... Thread 2... Thread 1 again ....
 * Makes sense for a multi threaded Java program. Results arent wrong, for each is just weird. 
 * To summarize - when using for each be careful! WIll execute the lambda on each partition in parallel and it will thread if on a single machine or it will be on nodes executing on their own
 * machine. 
 * 
 * What is correct answer? Its like what we did in previous chapter. Any action other than foreach will give right answers. Other actions will be implemented to give right answer. Although that
 * does not mean the actions are going to perform well. But in terms of correctness, any action will understand that the data is partitioned and that we will want the answers in the correct form. 
 * Ex - last chapter we used take ten method which took top 10 results. REgardless of partitioning, wont take ten from any random partition. Will be top ten across all partitions.
 * We need to perform some kind of an action as we did in the works example. 
 */
		List<Tuple2<Long,String>> results = sorted.take(100000); //exactly as we had it before and we will NOT get random data. Can do take with a big num like 100k and RDD will be sorted.

		results.forEach(System.out::println);

		
//		List<Tuple2<Long, String>> results = sorted.take(50);
//		results.forEach(System.out::println);
				
		sc.close();
	
	}

}
