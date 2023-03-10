//Lesson 53
package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Lesson53 {
	
	public static void main(String[] args) {

	
		//Lesson 53 Avoiding groupByKey and using map-side-reduces instead
/* From the last example where we looked at partitions and shuffles, is an example of a key skew. THis is when we have a lot of instances of one particular key and few instances of other
 * keys. We also want to avoid groupByKey as much as possible! Of course it depends on your requirements / situation. However, all performance guides will pretty much advocate to avoid
 * groupByKey. Just look at last example, we started with our data nicely partitioned across lots of partitions. 
 * (FATAL, Wednesday...) (Error, Friday...) this is one partition example. After doing groupByKey, we know that a shuffle is required! Not a good thing, avoid shuffles as much as possible!
 * 
 * Worst thing about groupbyKey is it stores all the values in a single node in the cluster. Recall data here is terabyte size. One of the nodes will have to store a third of that file
 * in memory!! We had 11 nodes in the cluster and now after the groupBy we will have a third of a terabyte in one cluster. Easy to end up with out of memory exception on our nodes. Even
 * if we have a bunch of nodes on the cluster. 
 * 
 * There are performance alternatives to group by key. Ex - lets say our requirement is to count num of FATAs, WARNs, and ERRORs. Not working because the node crashes from trying to fit too
 * many WARNs in memory.  So it will be more efficient to do a reduceByKey. We would do rdd.mapToPair() trick where we map each logging level to the number 1:
 * (WARN, 1), (ERROR, 1). Also recall this is a NARROW transformation so NO SHUFFLING required! Next we do rdd.(reduceByKey( (value1, value2) -> value1 + value2) 
 * reduceByKey does require a shuffle, so how is this better than group by key? Important difference: reduceByKey has TWO phases. First phase no shuffling required because spark
 * applies the lambda to the elements on EACH partition. It will apply the reduce first on each partition w/o shuffling. SO we end first stage with same num of partitions.
 * (WARN, 2) one partition. (ERROR, 1) (WARN, 1) another partition. Again no shuffling has occurred. Instead there will be a subtotal on each partition. There was two separate WARNs
 * originally so now we have the reduced value of WARN, 2. A more realistic value could be: (WARN, 234) one partition. (ERROR, 65) (WARN, 1345) another partition.
 * 
 * The reduce has been done on a partition by partition basis. (so again in the first partition we had two separate WARNs for our small value version) after reduceByKey we just have total count
 * of WARNs in THAT partition to be 2 just as an example). Again we will end up with partitions with a total of how many messages in that partition. 
 * For the bigger value version, that is called a map side reduce! Point of all of this is, we are NOT at final answer yet. We have to gather together the fatals into a single partition.
 * So we will be doing the shuffle NOW. Now the amount of data that is being shuffled is DRAMATICALLY reduced. There will be at most ONE entry for each key in our key space.
 * SHuffling is minimal. After shuffle, all FATALs on one partition, etc. (ERROR, 43) (ERROR, 563) (ERROR, 432). One partition, contains all ERRORs. 
 * 
 * Now the next thing is to apply the reduce again on the totals to get the FINAL results. (ERROR, 1103) one partition. Final result. Recap - reduceByKey will do a reduce on EACH partition 
 * first. This is our Map SIde REduce. This greatly reduces the amount of data shuffled around. So we will find that we can replace groupByKey in most situations with operations like
 * reduceByKey!
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
	//Q: How will initialRdd be partitioned?
	//A: 64mb blocks (at time of recording) so about 6 partitions
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");	
	//key: log level
	//value: date
		System.out.println("Initial RDD Partition SIze: " + initialRdd.getNumPartitions()); 
		
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair( inputLine -> {
			String[] cols = inputLine.split(":");
			String level = cols[0] + (int)(Math.random() * 11); //this is grabbing a random number and adding it to the end of our level such as `WARN6`.
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		//Now we are going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey(); //expensive operation with lots of shuffling
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2)+ " elements"));

		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		

		sc.close();
	
	}
	
}
