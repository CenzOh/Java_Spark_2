package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson79 {
	
/* Lesson 79 Setting spark.sql.shuffle.partitions
 * We can set a property called spark.sql.shuffle.partitions. It may not affect us in real life since it is again, the issue with small jobs in spark sql. When we go to spark structured
 * streaming section we will work with smaller spark sql jobs since we are working in small batches. One of the default configs works well with big jobs but not for smaller
 * jobs. May not affect us just yet, worth looking at though. Lets look at the spark UI again. NOTE - When doing a grouping, it will do a shuffle, so it creates some partitions of data.
 * Recall we have seen partitions earlier in the course. For each partition there will be a task assigned to the partition. 
 * 
 * In Spark UI we can see the job performed in 10 seconds so performance is good. However, second part of the job has 204 asks. Lets drill in to find out why.
 * So it shows 4 tasks were for the first part of the stage where it was reading in the date data which used 4 partitions. The 200 is weird lets address it. Drill in to that
 * and we can see the shuffle read size which tells us how much data each of the partitions are working with. THe max is fine with 822 bytes, some partitions worked with this amount.
 * 75th percentile also fine with 402 bytes. However, do you see that from median downwards it tells us a lot of partitions worked with NO DATA at all? Going further down we see
 * the whole story. Ex - partition 0 used 420 bytes but partition 1 worked with 0 bytes of data.
 * 
 * THe issue is due to the key size we are working with. We do not have that many keys in the output data. What we mean by this is the output from the shuffle is the result from
 * the grouping. We know from the output table that we have five logging levels and 12 months. So actually the results of the shuffle is that there will be 60 individual keys in the
 * output. SPark really only needs at most 60 partitions to do the job. So we have 140 partitions idle. May not really hurt us though ex - partition 1 took 90 ms to run. THis
 * time was used to start up, see theres nothing to do, and stop. Although we are not in a proper cluster, we run on desktop machine with 4 cores in the example which means
 * we can run four tasks truly in parallel. So the small 90 ms of doing nothing CAN add up. And we ARE heavily affected by this because some of the durations of these tasks
 * can be as high as 1 second. Entire job takes about 30 seconds to run even though it is dealing with tiny data.
 * 
 * So this is something we may want to configure. Stop the task and lets add it to spark session object
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		spark.conf().set("spark.sql.shuffle.partitions", "12"); //200 is default value for partitions every time a shuffle is performed. If we do 10 or 12 there should be no idle tasks
// good idea to make it a multiple of 4 since in the example we have 4 cores. My pc has 8 cores so I could do 16 if I wanted for no idle partitions
/* After running we can see the job now took about 7 seconds, not a massive impact HOWEVER, there were only 16 tasks. So this can be considered statistically significant.
 * Drill in and see there is a reasonable amount of data allocated to each task. We see results of:
 * 
 * Min 857 bytes. 25th percentile 857 bytes. Median 936 bytes. 75th percentile 1171 bytes. Max 1492 bytes.
 * 
 * Good spread. 
 */
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/biglog.txt");
		
		
// SQL reference
		dataset.createOrReplaceTempView("logging_table"); 

		Dataset<Row> results = spark.sql
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
			+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); 
		
		
		
		
		results.show(100);
			
// Java API / DataFrame version		
//		dataset = dataset.select(col("level"), 
//						  date_format(col("datetime"), "MMMM").alias("month"), 
//					  	  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
//	
//		
//		dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
//		dataset = dataset.drop("monthnum");
// 
//		dataset.show(100);

		
		Scanner scanner = new Scanner(System.in); 
		scanner.hasNextLine();
		

		spark.close();
	
	}
}
