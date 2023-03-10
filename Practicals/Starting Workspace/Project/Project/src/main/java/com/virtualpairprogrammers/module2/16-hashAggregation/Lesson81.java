package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson81 {
	
/* Lesson 81 How does HashAggregation work? 
 * TO learn about hash aggregation, we can check out the spark sql documentation. Its great and fast. However, at time of recording, there are not any real good information
 * when searching for hash aggregation. We get results for the java dpc but we want to see it in apache spark. For the java one, we found a page for SparkStrategies.HashAggregation
 * however, there is no detail in here. Its just a bad time of recording.
 * 
 * What if we come from a database background, being familiar with difference between hash aggregation and group aggregation? It is used by some databases.
 * If we search for this, we can see PostgreSQL shows up a lot and uses a similar strategy. Interestingly, reading the PostgreSQL reference document will tell us a lot more.
 * 
 * We will receive brief overview about these two algorithms and why do we see one for the java api search and the other is slower for SQL search. So, we have seen that Saprk SQL
 * can use two algorithms for grouping which is used for doing groupBy. Again, these are SortAggregate and HashAggregate.
 * 
 * SortAggregate, this groups by sorting the rows using a regular source algorithm and then it gathers together the matching rows. This is pretty much how we did it earlier
 * in the course when we grouped together the values for that row. Before the group, you get them in order in the first place to gather together the rows with the same level.
 * 
 * Level, Group of DateTimes
 * WARN, [2016-12-31 04:19:32, 2016-12-31 ...]
 * FATAL, [2016-12-31 03:22:34, 2015-4-21...]
 * 
 * BIG DOWNSIDE of this sort aggregation algorithm is that it has to perform a sort. They are not a slow solution but as the input set for the sort grows, the time it takes to do sorts
 * grows more than linearly. Performance: O(n*log n) but memory efficient. So if input data size is doubled, then the sort will take LONGER than TWICE as long.
 * Since we are dealing with massive datasets across partitions, there will be a serious penalty from doing sorts. In favor of sorts, they can be memory efficient. They are done in
 * place meaning if we have an input data set, we can really reorder the rows and we dont particularly need lots of extra memory to implement it. Depends on sorting implementation
 * but generally sorts are more efficient but performance will degrade as input set grows.
 * 
 *  There is an alternative algorithm called hash aggregations. Lets review the SQL statement. We will make one quick change to make the explanation of the hash algorithme asier to 
 *  perform (not changing the performance of the statement or it works under the hood). Just going to refactor it to be more like the java api version.
 *  We dont want to calculate the month num on the fly in the group by. We instead put this into the select statement and alias it as month num then do order by cast monthnum as int.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		spark.conf().set("spark.sql.shuffle.partitions", "12");

				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/biglog.txt");
		
		
// SQL reference
		dataset.createOrReplaceTempView("logging_table"); 

		Dataset<Row> results = spark.sql
//			("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
//			+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
				
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total, (first(date_format(datetime, 'M')) as monthnum "
			+ "from logging_table group by level, month order by cast (monthnum as int), level"); 

	
		results.show(100);
		
		results.explain();
		
/* Oops we forgot to drop the monthnum column. CHecking execution plan it still uses sort aggregates algorithm (the one we are familiar with). We made the change to show how 
 * hash aggregate would have worked if it had chosen to use that algorithm (again we know it didnt and we will find out why shortly).
 * If it were to use the algorithm, this is how hash aggregation would have worked. It has a very elegant algorithm for grouping:
 * 
 * Raw input data:
 * Level=WARN	month=February	monthnum=2
 * Level=INFO	month=December	monthnum=12
 * 
 * Recall all of this occurs under the hood. Spark SQLs job is to do a grouping. We have done a select of the level month and we generated a monthnum on the fly. Above is what the raw
 * data would look like, differnet levels and months. Spark SQL goes through these millions of lines and groups them together. An obv way is to sort the rows by level and month then
 * gather them up together. Hash aggregation will create a table of key value pairs. Think Hash Map from java. But under the hood in spark this is NOT a hash map. There is a series
 * of key value pairs and we can insert rows into this table. Inserting rows are fast and efficient operation.
 * So Spark SQL looks at first line of input data and builds a key which will be built from all the grouping columns. We group by level and month so spark SQL will create a key
 * something like this, a composite of the two values. Just like in the java hash map, the process of creating a key and inserting it into a hash map is efficient.
 * The way it works is it takes in the key, a simple string, then runs through hash algorithm which converts it into a number, use the number to index the array. Indexing an
 * array is super fast operation. 
 * 
 * "Key" 				"Value"
 * WARN:February
 * 
 * It added an entry for WARN:February. The value will be the rest of the data that is NOT part of group by clause. For us this will be the monthnum. Note, we are also doing an 
 * aggregation as well, to calculate the total so that too will become part of the value. Again, the value is all the data for that row that is NOT part of grouping key. It will run
 * through the table line by line. For second line, this is first time we see INFO:February so this lines total will be 1. 
 * 
 * "Key" 				"Value"
 * WARN:February		monthnum=2, total=1
 * INFO:February		monthnum-2, total=1
 * DEBUG:December		monthnum=12, total=1
 * 
 * Now for the fourth input data, we have seen this key already for
 * WARN:February. When it does the hashing it will detect that there is a collision, an entry in the table exists.
 * THIS IS IMPORTANT: This is DIFFERENT to a java hash map, what spark will do is go in and modify the value / overwrite the total value. We will see why this is important shortly.
 * 
 * "Key" 				"Value"
 * WARN:February		monthnum=2, total=2		*** total was updated
 * INFO:February		monthnum-2, total=1
 * DEBUG:December		monthnum=12, total=1
 * 
 * Now Spark SQL will go through the rest of the input data, following the rules, this is what the final result will look like after the new values and duplicates / collisions:
 * 
 * "Key" 				"Value"
 * WARN:February		monthnum=2, total=3		
 * INFO:February		monthnum-2, total=3
 * DEBUG:December		monthnum=12, total=3
 * 
 * Very elegant operation. Think about the performance, it does ONE pass through entire dataset all the way down. Performance is O(n). So if we double the size of the data set, the hash
 * aggregation function will take about twice as long which is way better than a sort. We dont get anything for free. The expense is the memory. In our example this will not be
 * an issue since our key space is small. So small output table. Using hash aggregation instead of sort aggregation is great for optimization.
 */


			
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
//		
//		dataset.explain();

		
		Scanner scanner = new Scanner(System.in); 
		scanner.hasNextLine();
		

		spark.close();
	
	}
}
