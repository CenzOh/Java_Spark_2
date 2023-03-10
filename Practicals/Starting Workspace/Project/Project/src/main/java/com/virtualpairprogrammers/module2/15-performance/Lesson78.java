package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson78 {
	
/* Lesson 78 How does SQL and DataFrame performance compare?
 * We can see the work in achieving the requirement was 12 seconds of Spark SQL time. There will be some overhead in JVM to initialze spark. So actual runtime will be longer than 20 seconds.
 * Dont forget to keep terminating. Now lets bring back the SQL API and comment out the Java API. We know its a bit different but we can check if it will improve performance.
 * After the test we can see java API took about 20 second run time, but the sql API took longer.  Lets check spark UI, says the sql job took about 41 seconds. COmpared to the
 * 12 seconds from before (looking at spark ui). So is SQL API slower?? No not exactly, lets elabortate:
 * 
 * One of issues when working with small data (our case this is small because it can fit on one machine) is sometimes very minor fluctuations can cause a BIG skew of results.
 * If we ran this on Elastic Map Reduce (EMR) and did the same steps, same process, things will be different.
 * 
 * Input: 80 million log records (~2 Gb input) still not big data but bigger than what we had.
 * Cluster: 4 workers, c5d.xlarge, using a bigger one since there are a new generation of generation types for EMR. 
 * SQL version | Java API version
 * 7.9 min     | 1.9 minutes
 * 
 * QUICK NOTE that even tho we use the java api it is STILL using Spark SQL. Ok so same results. Java is 4x faster!? 
 * Instructor will let us know in later chapter why this is the case. because java api is better than actual syntax? Will find out soon.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/biglog.txt");
		
		
// SQL reference
		dataset.createOrReplaceTempView("logging_table"); //need the temp view for SQL API

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
