package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Lesson64 {
	
/* Lesson 64 Groupings and Aggregations
 * We will look in more detail at grouping and aggregations, we did a lot in the first section of the course with RDDs. Now we will use SQL syntax
 * We want to do some reporting on this log file and it would be interesting to find out how many WARN messages we have in the log, etc. Do this with grouping.
 * How does grouping work? Well first, requirements are to gather all WARNs, FATALs, etc. Then it counts each of the types of levels that we had.
 * Key word in SQL called Group BY lets us do this. Ex - select level, datetime from logging_table group by level (wont compile, this is the point to add we want group by the level)
 * So now our output will show: WARN, FATAL, INFO. Groups the same levels together. THis right now is an order by. Group by will ALSO combine the results into single rows
 * with ONE row for each level. Conceptually: WARN, [206-12..., 2016-12...] etc. One row for each loggin level. Now NOTE that the values here are not single data anymore. It is an
 * aggregation of all the dates that were found for the logging level. Can be represented by putting into square brackets. Lets see actual results.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		List<Row> inMemory = new ArrayList<Row>(); //import list and arrayList from java.util

		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32") );
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34") );
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21") );
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21") );
		inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20") );
		
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty() ),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};
		
		StructType schema = new StructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(inMemory,  schema); 
		
//make a call on the dataset all the .createOrReplaceTempView
		dataset.createOrReplaceTempView("logging_table"); //can refer to the table as logging_table now
//		Dataset<Row> results = spark.sql("select level, datetime from logging_table group by level"); //can return as a new dataset. Running this crashes!
		
/*Now what was not explained is that when we do the grouping we end up conceptually at least with a group of dates, times. We dont really want a collection of these values, rather
 * we would want a calculation which are called aggregations. THis is when we have a group of some values from the table and we want to do an operation such as counting how many of them
 * there are. Ex - WARN, 2. INFO, 1. etc. so when we do the grouping we need to specify an aggregation function against whichever one of the columns that is being aggregated.
 * Ex - select level, count(datetime) from logging_table group by level. See how we aggregate the datetime with count() function
 * 
 * Even tho we group by level, it will return one row for each distinct level that was found.
 */
		Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level"); //aggregating datetime with count()
//		Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level order by level"); //alpha order
		
		results.show(); 
		
/*
 level | count(datetime)
  INFO | 				1
  WARN | 				2
 FATAL | 				2
 
 It works. The ordering is random here can do a `order by level` for alpha order.
 Count is an example of an aggregation when we do a group by you can go to spark documentaiton, API Docs > SQL Built in functions. Very useful.
 */
		Dataset<Row> results1 = spark.sql("select level, collect_list(datetime) from logging_table group by level");
		results1.show();
		
/*
 level | Collect_list(datetime)
  INFO | [2015-4-21 14:32...
  WARN | [2016-12-31 04:19...
 FATAL | [2016-12-31 03:22...
 
 Columns get truncated if they get too long. These are a collection of values for datetime. True with collect_Set to group elements together. We will be working with big data so the collections
 will be massive. Not a common operation. But this is just like groupByKey() operation recall from section 1. Grouping by key with big data is a dangerous operation!
 */

		spark.close();
	
	}
}
