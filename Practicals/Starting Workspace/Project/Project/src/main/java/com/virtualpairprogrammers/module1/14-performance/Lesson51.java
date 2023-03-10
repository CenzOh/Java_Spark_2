//Lesson 51
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

public class Lesson51 {
	
	public static void main(String[] args) {

	
		//Lesson 51 Shuffles
/* Shuffles are always an expensive operation. Best advice would be to avoid shuffling. SIlly tho since we cant avoid them in any non-trivial spark job. If you need to do a shuffle, 
 * then you need to do a shuffle. Think carefully WHEN you do a wide transformation in our job. Ex - get a report that counts num of fatal log messages over period of this log.
 * We could have done the steps we had seen. And now we could have gone in, get key of fatal, count num of elements, get answer!
 * 
 * Although there is MAJOR performance improvement we can make! We did the shuffling and then ignore all the WARNs and ERRORs just go straight to FATALs. More sensible opt is to 
 * ignore the others in EARLY stages of job since we are interested in fatals. Do this:
 * filter (line -> line.startsWith("FATAL"))
 * This would have ended with an RDD that has two FATALs in two diff partitions and the other 4 partitions are empty. Smaller amount of data and we can do whatever grouping
 * we want! So by doing groupByKey() we have less data to shuffle. With more complex spark jobs, may not be easy to twll when we have done a wide transformation too early.
 * Good reason why Spark UI is helpful guide to where we have made some performance blunders.
 * Lets write this in code, instructor called the file ParitionTesting, Ill write it here first.
 * Apparently this file should be included and the bigLog.txt under resources.
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
		System.out.println("Initial RDD Partition SIze: " + initialRdd.getNumPartitions()); //outputs num of partitions spark determined for RDD
		
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair( inputLine -> {
			String[] cols = inputLine.split(":"); //remember this is a narrow transformation, NO shuffling
			String level = cols[0];
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		//output num of partitions AFTER transformation, num should be same, no repartitioning going on.
		
		//Now we are going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey(); //expensive operation with lots of shuffling
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		//output num of partitions after wide transformation. num doesnt change again its just that most partitions will be empty
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2)+ " elements")); //loop through each key and output elements in resulting value
		
/* If we have a previous run in console, make sure to hit return and the stop button should NOT light up. Otherwise cant get the web UI. Lets run it!
 * After running, we can see it took several seconds to run.
 * 11 partitions on EACH sys out
 * 
 * key WARN has 499886 elements
 * key ERROR has 5001114
 * 
 * We were told on the chapter with Amazon EMR that what we see here is logging. THe 0 is the num of completed tasks. WE have 11 tasks (total) so there were 11 partitions.
 * (0 + 4) / 11
 * (1 + 5) / 11 num of tasks completed keeps inc. Now look at stage 0 and 1. Look at spark UI to see what stages are about.
 * Also note after wide transformation it is totally possible that two of our 11 partitions actually have data in them. Can prove with web ui. Look at local host 4040.
 * This time it is simpler than before. We have one completed job and thats because we had ONE action. The action was the for each.
 * Click on it to drill in. The DAG as we have seen previously, each box is a transformation, we see: Stage 0, textFile -> map -> Stage 1, groupByKey -> mapValues.  
 * 
 * Our initial transformation is loading text file. If you click on the boxes it will take you to an expanded view. This will point us back to the line of code that resulted in
 * that transformation. Tells us caused by line 28. Important thing to see what all the stages are about. A stage is a series of transformations that DONT need a shuffle.
 * When we get to the point where a shuffle is required then Spark creates a new stage so we can immediately see at a glance here that our Spark job needed 
 * one shuffle instance the stage got as far as the map. Map comes from line 32. Map was a narrow transformation. So it was done in same stage as previous.
 * 
 * In next transformation was groupByKey(). Shuffle occurred AFTER the map. UI doesnt explicitly show it. If we look at teh stages, they have names. Comes from last transformation.
 * Can get a feeling how long it took. Says took 6 seconds. We had 348 mb of data. THis is the size of input file. Can see the shuffle write at end of 79.7 mb. When it writes data
 * to disk, it can be serialized before being transfered across the network. Internal implementation of that is: data is compressed. That is why what we see is much smaller than input.
 * Although we did NOT remove any data before shuffle.
 * 
 *  Significant compression of data. Look at next stage, stage one. First thing it does is read data back in with shuffle read of 79.7 mb. Thats how it is implemented. Called a 
 *  push-pull model. Each stage outputs its results and next stage reads them back in. Lets look at stage 0. Go further down we can see a lot of info. There were 11 tasks.
 *  Avg duration (median) 2 seconds. Min (fastes) 1 second. Slowest (max) 3 seconds. THis is good because there is no big spread across the times. We have line for GC (garbage collection)
 *  which should not be an issue. But if we see big values sticking out, wwe may have an issue creating too many objs in the task.
 *  
 *  Similarily we can see input tasks were 30 on avg. Looks like one of the partitions did not get the complete block of 32 mbs, that one had a low value of 28.1 mb. We can ALSO find
 *  this out by scrolling down to the tasks. We can see the tasks which are java threads executed against the 11 partitions. ALl successful. Durations displayed all
 *  except last one had input size of 32 mb. Shuffle write each had 7 mb or so of data. IRL, up to us to determine if values are good or not.
 *  
 *  Next stage, stage 1 with group by key. In line 42 of code, wide transformation. Recall we still have 11 partitions / tasks. Can see duration min time is 3 ms.
 *  Median is 29 ms. Max is 9 seconds. Woah alarming. Wide spread of values. THis suggests some tasks do NOTHING while others work hard. Drill down more, we have eleven tasks.
 *  Turns out tasks six and seven got ALL the data so we ended up with a partition containing all WARN keys and another containing all the ERROR keys.
 *  All other partitions are empty. Other tasks took some time to start up, see there was nothing to do, turn off. All those that did nothing read nothing in shuffle read.
 *  
 *  IRL we may think to deploy this to a cluster with 11 worker nodes. Good parallel performance? Well thats fine for first stage, but second stage most of cluster would
 *  still be idle. Is this an issue? Depends on real world dataset. THis concern though for second stage (stage 1). We are experiencing possibility of our cluster being underutilized.
 *  Look at this: if stage 0 takes 90 percent of time and stage 1 takes 10 percent of time, not really an issue since there would be short amount of time spent in stage 1 (second one).
 *  If inverse was true, stage 0 takes 10 percent of time and stage 1 takes 90 percent of time (IE 90% of time cluster is idle) then yes we would have rework to do.
 */

		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		

		sc.close();
	
	}
	
}
