package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson82 {
	
/* Lesson 82 How can I force Spark to use HashAggregation>
 * Why does Spark SQL seem to be using sort aggregation sometimes and hash aggregations other times? The strategy is (in general) hash aggregation is MUCH faster than sort aggregations.
 * Its about 4x faster in performance. Hash aggregation d/n need to do a sort but does need more memory. SOmetimes can be an issue. In our example, no problem.
 * We dont have control whether Spark will use sort or hash aggregation other than the fact that Spark SQL will use hash aggregation where possible.
 * What do we mean by where possible? At the time of recording, the documentation is abysmal, there is no obvious answer in the docs. Our instructor tells us that Spark will only do 
 * hash aggregation if the data for the value in that table (month num and total counts for us) is mutable. 
 * Maybe we are familiar with immutability concept in Java but that doesnt seem to have anything to do with Spark SQLs concept of mutability. So what does it mean?
 * 
 * First lets point that the hash map we have been talking about is NOT actually a java hash map from the java api. Spark decided to implement this hash map or hash table
 * using native memory. For reference this is what our hash map / table looks like:
 * 
 * "Key" 				"Value"
 * WARN:February		monthnum=2, total=3		
 * INFO:February		monthnum-2, total=3
 * DEBUG:December		monthnum=12, total=3
 * 
 * So this means they are not implementing this using java objects. The table is not being stored in the regular java heap. Instead they effectively leave the JVM, grab a chunk of real
 * memory and read / write to that memory directory. More info on apache spark git hub repo and there is a class called UnsafeRow.java. Under 
 * spark/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/ The Unsafe refers to a library from standard java. Unofficial extension to java. The library is called
 * sub.misc.Unsafe. Unsafe is a class, not really in Java API since it is undocumented but is part of the Java development kit for a number of versions. This allows us to do what
 * Java considers, unsafe operations. This means we are leaving the confines of the JVM and we do things like reading and writing to raw memory.
 * 
 * The sun.misc.Unsafe class has been a bit controversal for many years and has been removed in Java 9 and will not be replaced. As of the recording, spark will not run on java 9
 * because it is using that unsafe class. So the UnsafeRow is using the concept of the unsafety in java. This is a row backed by raw memory instead of java objects. No garbage collection.
 * This is an advanced performance optimization they tried to do. 
 * 
 * Recall how when illustrating the algorithm we went in and updated the total by overwriting with a new number. Spark can only do this if the data type used any of these values
 * are over writable. There are only a small number of data types that are over writable. Spark calls these mutable data types. We can find out which types are supported in this
 * UnsafeRow class. The following are field types that CAN be updated in place in UnsafeRows:
 * Null, Boolean, Byte, Short, Integer, Long, Float, DOuble, Date, Timestamp. Decimal as well but its not in the list. If our data types in the value for the table are not included in that
 * list then we are out of luck and cant use the hash aggregation and it will fall back to the slower sorting aggregation.
 * 
 * Thats the answer, to use hash aggregation, ensure that all the data not in grouping clause (not the key) are mutable type. In our example, the total is integer and mutable, good.
 * Why does this not work for us? Has to be monthnum value. And we can see the monthnum is being called by date_format method on datetime and it returns a string. So
 * monthnum is string type and we cast it back to an integer later. But it means we're actually seeing the month come back as a string, which is not mutable.
 * 
 * "Key" 				"Value"
 * WARN:February		monthnum="2", total=3		
 * INFO:February		monthnum-"2", total=3
 * DEBUG:December		monthnum="12", total=3
 * 
 * Why is string not mutable? Because storage space required. An int always takes 32 bits. Spark can always overwrite the int because it wont need more than 32 bits. Long is 64 bits.
 * Fixed width. String is variable width. We can overwrite string with a completely different value and can be arbitarily long and spark would then have to shuffle data around
 * and cant perform.
 * 
 * Now we know we can influence things so that spark does hash aggregation. Lets refactor the query. Several ways to do it. One option is to do the recast immediately when calling
 * the date format function. We know that in the `date_format(datetime, 'M')` returns a string, cast this as int.
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
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total, (first( cast(date_format(datetime, 'M') as int) ) as monthnum "
			+ "from logging_table group by level, month order by monthnum, level");  //change here
		results = results.drop("monthnum");
		
/* After running program after change, took about 33 seconds rather than over a minute. Much faster now. Also with the .explain() we can see that hash aggregation is in use.
 * Oh and lets drop the monthnum column, dont need it. There are many ways we can do a query. Comparing it with java api version, we included the monthnum in groupby clause.
 * This will put monthnum onto the key. Pretty much removes the issue. Doesnt matter if the key is mutable its the value thats important.
 * 
 * "Key" 					"Value"
 * WARN:February:"2"		total=3		
 * INFO:February:"2"		total=3
 * DEBUG:December:"12"		total=3
 * 
 * This is actually what has been happening in the java api version throughout the course. Same results. 
 */
		results = spark.sql
			("select level, date_format(datetime, 'MMMM') as month, count(1) as total, date_format(datetime, 'M') as monthnum "
			+ "from logging_table group by level, month, monthnum order by monthnum, level"); //including monthnum in group clause. We have monthnum initially as string but if its in the key
		results = results.drop("monthnum"); //spark will use hash aggregation instead of shuffling
	
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
