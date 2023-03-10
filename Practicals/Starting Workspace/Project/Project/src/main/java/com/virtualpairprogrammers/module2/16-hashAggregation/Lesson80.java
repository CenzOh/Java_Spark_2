package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class Lesson80 {
	
/* Lesson 80 Explaining Execution Plans
 * So we have seen according to the results, spark sql syntax performs 4 times slower. However, instructor claims this is a near approximation to same funcitonality expressed in java api.
 * He argues spark syntax is NOT slower. Most cases have no appreciable difference in performance between the two. But our results are not showing that. 
 * Can find out the clues in spark UI. There is another way to investigate our issue. Comment out the scanner, we can call .explain() on our dataset which is called results. 
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
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
			+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); 
		
		
		
		
		results.show(100);
		
		results.explain(); //will write to system.out.println and show us exeecution plan for data set.
/* Looks something like this:
 * == Physical ==
 * *(3) Project [level#10, month#14, total#15L]
 * +- *(3) Sort [aggOrder#45 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
 * ...
 * 
 * Yeah its a bit difficult to read. Although it is telling us, from reading it bottom upwards. So first thing is all the way at the bottom where it says FileScan.
 * For each step with an asterisk alongside, that will be part of a wholeStageCogen step. THe bottom lines may combine into a codeGeneratedStep. Both say *(1)/
 * We can presume that this is part of the grouping. Not the final order by, but by the grouping. Super hard to read. But this is saying to sort on the level. Might be group by clause.
 * When grouping, spark gathers all the rows where the level and month are the same and puts them together. What happens here is called sort aggregates. It uses java api
 * and compares the two plans. Lets see what the plan would look like for the java api version.
 * 
 * We get another physical plan, looks pretty similar. However there are some significant differences. Look at the cols. Labels on the steps are important. After doing fileScan of CSV
 * the steps are the same. But the two plans are completely different. The two sorts are the final sort, thats the order by clause. The Project (not like work project but like projection stage)
 * its a select statement. We select the level of month and total. Where we did drop we dropped the month num and we can see that at end of result. Stuff in the middle varies dramatically.
 * There are two sorts in the SQL version and a stage called sort aggregate. Java API version has two steps but they are called hash aggregates. No reference to sorting. There
 * is still order by sort, but no sorting to achieve grouping. Some databses use this thing called hash aggregation. Will look at next time.
 */
			
// Java API / DataFrame version		
		dataset = dataset.select(col("level"), 
						  date_format(col("datetime"), "MMMM").alias("month"), 
					  	  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
	
		
		dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
		dataset = dataset.drop("monthnum");
 
		dataset.show(100);
		
		dataset.explain();

		
		Scanner scanner = new Scanner(System.in); 
		scanner.hasNextLine();
		

		spark.close();
	
	}
}
