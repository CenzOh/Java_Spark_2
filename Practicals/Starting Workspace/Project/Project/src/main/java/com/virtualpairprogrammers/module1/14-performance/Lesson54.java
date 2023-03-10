//Lesson 54
package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Lesson54 {
	
	public static void main(String[] args) {

	
		//Lesson 54 Caching and Persistence
/* We will demonstrate this with a simple example, the partition test. Terminate the program and lets update the code below: Edit the line with String level = cols[0]
 * So note that we dont have the salting anymore, avoid it unless necessary in real life! So we are running through our transformation and counting how many elements for each key.
 * What if we want to get a grand total? See below
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
			String level = cols[0];  //this line to remove the salting
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		//Now we are going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey(); 
		
//		results = results.cache(); //review this after bottom blurb. THis stores the RDD results for future ops so we dont have to go back to results of previous shuffling
		results = results.persist(StorageLevel.MEMORY_AND_DISK()); //takes a parameter of type storage level
		
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2)+ " elements"));

		System.out.println(results.count()); //simple print out total! HOWEVER, big problem here. Serious performance issue
/* Very subtle to spot. Run the program and look at web ui. But first for our output we get"
 * key ERROR has 5001114 elements
 * key WARN has 4998886 elements 
 * 
 * so we have the two stages as usual and it outputs elements for the keys which is what we wanted. HOWEVER, when we call count it runs ANOTHER stage. So this suggests when we do count
 * we had to do a shuffle? Why? Well lets look at the spark UI. We see a list of all the actions that were performed. So we see:
 * Job ID | Desc   | Stages   | Tasks
 * 1	  | count  | 1/1	  | 11/11 Both stages and tasks here show `1 skipped` and `11 skipped`
 * 0	  | foreach| 2/2	  | 22/22
 * 
 * We only had ONE entry which was the for each. But now we have a separate entry for count. Foreach is the same as before. But in the count we can see this is the stage 3 we did not
 * expect. It has done a groupByKey AGAIN. It also has a stage 2 which is skipped. WE'll come back to that. But why is it running another stage? Looks like it is redoing
 * previous steps that were already done. If we look at the first job (ID 0) we can see it already did groupByKey and mapValues. SOmething wrong here.
 * 
 * SO this comes back to the fact that when we have a Java RDD we dont really have any data at all. Again, no data is loaded in the JavaRDD<String> initialRDD. We are again, adding elements
 * to the execution plan. It is only when it reaches an action such as foreach that it becomes a job and executes the plan. The foreach is our first action. So it will do the
 * transformations, shuffling, grouping, then output the results. Once it outputs the results, the real RDD in memory / actual data will be discarded. So now, when it comes to the counts,
 * it has to go all the way back to execute those steps AGAIN. 
 * 
 * Let us address the concept (we could not have talked about it earlier since we needed to understand and work more with spark before addressing it), when we get to an action, it has to go
 * back to the initial RDD to rerun. It does not store the intermediate data in memory. If we go back and look at the counts job, the second job carried out, it goes back to the beginning
 * of loading in the text file and run those stages again. You can see it actually skipped stage 2 (of loading the textFile and doing the map). This is a performance optimization that was added 
 * way back in Spark 1.3 something like that. Whenever a shuffle occurs (which we know it happens here at the end of stage 0, the results of the shuffle are written to disk). 
 * 
 * This means when it went to the second job, it does have to go back to the beginning but it is able to optimize because it knows it already done this and written the results to disk so 
 * that stage can be skipped. Stage 3 can simply read that shuffle back in some memory. SO not entirely true that it always has to go way back to the start. In fact, for an action to occur,
 * it is going to have to go back to the last shuffle stage, basically. 
 * 
 * So in this example, to do the count, it had to do the groupByKey again. The mapValues in the UI is interesting because we DONT have a mapValues in our code, its just part of the groupByKey.
 * Its an internal implementation detail. If you follow the link you can see that (shows its executed on same line as our groupByKey(). Point is, it had to do group by key AGAIN 
 * when we have already done groupByKey. May seem inefficient that every time an action is performed Spark goes back to the results of the previous shuffling. Easy to end up doing more work
 * than we are doing in our scripts.
 * 
 * How to avoid the inefficiency: tell spark we want to store the RDD in memory and keep it for future operations. We can specify this after the groupByKey(). See above
 * results = results.cache();
 * It is truly that simple. Recall, this is a method that returns an object so we have to reassign. Again, this tells Spark that the actual data that results from the transformation needs to
 * be a check point of sorts and stored in memory. This will not run the groupBy again.
 * 
 * After we stop the program and run it again, you can see there is a warning: WARN MemoryStore: Not enough space to cache rdd_4_7 in memory. Unfortunately, cache will work only if we
 * have enough space in RAM. If cache fails, it will give the warning and carry on as before. The stage 3 did have to run the group by again. Cache is good for small RDDs.
 * An alternative for cache is persist(). Takes type storageLevel. Not very readable but we have options of disk only, memory only, disk and memory. If we go for memory ONLY, we will
 * get same thing as we did with cache. Same operation. We can use memory and disk. Will first use as much memory as it can then falls back to disk.
 * results = results.persist(StorageLevel.MEMORY_AND_DISK());
 * 
 * This time when we run again, our warning says: WARN MemoryStore: Not enough space to cache rdd_4_7 in memory (computed 366.1 MB so far)
 * WARN BlockManager: Persisting block rdd_4_7 to disk instead. Does exactly as we expected. Can see better difference with bigger data set. In Spark UI, we can see in the first job
 * for each, the difference is when we get to mapValues theres a green circle saying it was cached. Job 1 still shows Stage 3 but the green circle shows that the RDD is being read
 * from the cache so the groupByKey step would have not needed to take place. 
 * 
 * Lets actually take a look at the viewingFigures example to see how we can use this for further optimization. Recall we had a complicated set of transformations. Before closing spark context, we
 * will do the scanner trick. Then we will run the program to see how it looks in spark console. Refer to ViewingFiguresOptimized.java
 * 
 * This code is certainly more complicated. We have several stages, it goes up to stage 6. You can see from the logger it expresses stage 6 has to run in parallel with stage 1. THis is what Spark
 * will do. It will check if one stage is dependent on another stage or not. If not can run in parallel. Thats the point of the DAG / execution plan. Spark runs lazily so it can do optimizations
 * like this. If you look at this on localhost4040 we can click on Job 0 with sort by key. And wow there are a LOT of stages that get shown to us. Super complicated look. 
 * 
 * So, stage 0 points to line 103 where it reads in the titles of the courses textFIle. In stage 1, it reads in the viewingFigures. Stage 2 reads in the chapters.csv data. We have jobs where
 * we do joins like in Stage 4. Occurred on line 47. We join the viewingFigures and chapter data. Stage 4 simply depends on the previous lines. Depends on the two sets of input data (stage
 * 1 and 2). Recall in stage 1 where we input the viewingFigures we had to remove the duplicated views. 
 * 
 * When looking through there is something quite odd occurring. We only have three input text files. One stage reading each in. But in Stage 6, one of the files is being read again?
 * Clicking on the link, this occurs on line 133, it is reading in the chapters data. This is odd since we got this data on stage 2. Again, this has to do with the lazy evaluation of
 * spark. When we do a transform, the input data from the transform is NOT stored in memory. Lets look at it again. Stage 2, first transformation applied to the text file. the map is 
 * happening on line 134. The map is the splitting of the data into a pair RDD and the results are then passed into the join in stage 4. The data in the chapters file is needed so it 
 * can do the join but is needed much later in the process.
 * 
 * In stage 7 we have the reduce by key operation on line 39. This is where we worked out how many chapters for each course. So we are reading the chapter data on line 39 but we are also 
 * doing a transformation on line 47. And as we know, the data in these RDDs are not by default cached unless we force Spark to do so. The point of all of this is that in another
 * point of the process we needed the input data again. We needed it in two separate places in our script. We may have not spotted it unless we looked at the graph.
 * 
 * Solution? If we hover the map operation in stage 2, we can see that it is the same as the map operation in stage 6. So we should be caching the results of the mapping operation on line 134.
 * This is where we do the map to pair. What we can do at the end is add the cache so it can be stored in memory.
 * 
 * Now after rerunning the code, we can see a bit of a difference. Will certainly be more impactful on larger scale data or on operations where we do 15 or so transformations before reusing the
 * original CSV. WE have been doing Spark RDDs / Spark Core. Next chapters we'll be looking at a new API, Spark SQL.
 */
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		

		sc.close();
	
	}
	
}
