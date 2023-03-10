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


public class Lesson66 {
	
/* Lesson 66 Multiple Groupings
 * Lets go further with groupings. Let us group on multi columns and then use a realistic big data set. Now we have a data set with a boiled down form of log messages.
 * All we have is the level and month of message. WE have small set of data but it is true we will have duplicate values like WARN December. THis means we had a warning log message
 * on some day in December of some year. ANd on another occasion, we had another warning message ALSO in december. It could have been the same day as first message. Maybe a different day
 * or even a different year.
 * 
 * Our requirement is to just work out how many messages of each level we were getting each month. So what we have to do is count up duplicates. That is anohter job for grouping but let
 * us group on multiple cols. SO if we think about it, we might think to group by level, month so we can group by both. Spark will gather duplicate rows and this time
 * both values (cols) have to be the same! Only WARN, December will be grouped together, others are distinct.
 * 
 * ALthough you may have noticed, this will NOT WORK because when we do a group we must do an aggregation on a col that is NOT one of the cols we are grouping on. But we pnly have two
 * cols we are grouping on. Common trick is to add in a dummy col with a number 1 on it. Level, Month, 1. WARN, December, 1. To do this in SQL code we do: select lvel, 
 * month, count(1) from ... that simple! Select hard coded value 1 and count instances of 1. SO then WARN, December will have 2 and no duplicate
 * There are many ways for us to get the right answer.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		List<Row> inMemory = new ArrayList<Row>(); 

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
		
		dataset.createOrReplaceTempView("logging_table"); 
		
		
		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table"); 

		results.createOrReplaceTempView("logging_table"); //reusing same table name will overwrite the previous temporary table
//		results = spark.sql("select level, month, count(1) from logging_table group by level, month"); //we can call col as month because month appears in the new dataset
		results = spark.sql("select level, month, count(1) as total from logging_table group by level, month"); //renamed the count() col to total
		
/* Note we could have done this in a single line of SQL check if works first
 * level | month | count(1)
 * WARN | December | 2
 *...
 */
//		results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month"); //single line of sql
		
/* In our extras folder there is a file called `bigLogs.txt` this file contains one million log messages. If try to open in standard text editor, may crash. Put the file in src/main/resources
 * then refresh the folder
 */
		Dataset<Row> big_dataset = spark.read().option("header", true).csv("Src/main/resources/bigLog.txt"); //spark obj, read in a file, .option to read in headers, .csv add path returns dataset
//of rows and we will call the obj big_dataset
		
		big_dataset.createOrReplaceTempView("logging_table"); //replacing the view with big data set
		results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month"); //one line for full month and total count
		
/*
 level | month | total
 WARN | June | 8191
 ...
 
 Took a bit longer but results work. We get top 20 results. How to get full results? with .show() method, specify how many rows you want to see in output. .show(100) is safe to do.
 .show(false) will actually not do anything different. Next lessons we will make output more presentable. 
 */	
		
		results.show(); 
		
		spark.close();
	
	}
}
