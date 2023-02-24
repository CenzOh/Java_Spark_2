package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson83 {
	
/* Lesson 83 SQL vs DFs Performance results
 * Recall the results about running on a realistic cluster and how long it took with 80 million log records, about 2 Gbs worth of data with 4 workers in the cluster using the 
 * c5d.xlarge system.
 * 
 * SQL Version	Java API Version
 * 7.9 minutes	1.9 minutes
 * 
 * Now we know the REAL reason has NOTHING to do with java vs sql but instead it has to do with the underlying grouping algorithm used. SQL version accidently used sorting aggregation
 * and java used hash aggregation. And the reason we got the hash aggregation in java api version is because we grouped by level, month, and monthnum at that time. 
 * If we run the same test again with SQL using hash aggregation, what will the results be?
 * 
 * SQL with HashAggregate	Java API Version
 * 2.1 minutes				1.9 minutes
 * 
 * No appreciable difference between the two APIs. No statistically difference between the two.
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
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total, date_format(datetime, 'M') as monthnum "
			+ "from logging_table group by level, month, monthnum order by monthnum, level");
		results = results.drop("monthnum"); 
	
		results.show(100);
		
		results.explain();
	

			
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
