package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class Lesson71 {
	
/* Lesson 71 Coding the pivot table in Spark
 * Pivot table on two separate cols is a more compact way to display the same results. WE will have five logging levels with 12 months. 5x12. 16 cells. Rather than 60 rows previously
 * We cant pivot a table with SQL syntax. The SQL language syntax comes from Apache Hive, which Spark is built on top of. According to their documentation, there are no reference to pivots
 * as of the recording. We can still do part of the work with Spark SQL syntax. We can do select for intitial data, then when we want pivot, jump into java api. For now lets stick with java
 * api. 
 * 
 * Always start with first grouping. Recall we will group on level and month. Start with grouping on what we want to be rows (left hand col) that would be levels
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> big_dataset = spark.read().option("header", true).csv("Src/main/resources/bigLog.txt");
		
		big_dataset.createOrReplaceTempView("logging_table");	
		
// SQL reference
//		Dataset<Row> results = spark.sql
//				("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
//						+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); 
		
// Java API / DataFrame version
		
		big_dataset = big_dataset.select(col("level"), 
				date_format(col("datetime"), "MMMM").alias("month"), 
				date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
		
//		bad_big_dataset2 = big_dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
//
//		bad_big_dataset2 = big_dataset.orderBy(col("monthnum") );
//	
//		
//		bad_big_dataset2 = pivoted_big_dataset.drop(col("monthnum")); 
//		
//		bad_big_dataset2 = big_dataset.orderBy(col("monthnum"), col("level") ); //handled in pivoting
		
		Dataset<Row> bad_big_dataset = big_dataset.groupBy("level").pivot("month").count(); //no pivot option initially until we call .groupBy(). Then call our aggregation method which is count()
//note when we call pivot we are doing the second grouping
		
/* OUTPUT:
 * level | APril | August | ...
 * INFO | 29302 | 28339
 * ...
 * 
 * It works but is not sorted correctly. Quick note but when we do a pivot it will do a natural ordering for the col on the pivot in our case that is the month column so it will be in alpha order
 * We can see on here generally FATALS are a lot smaller in general. Lots of info on DEBUGs. Only FATAL that is high is in November. All below 100s but November is higher. Note this is summarizing
 * EVERY YEAR. So we would have to check if it was one particular year. Maybe something bad happened every black Friday? Will look at in the future.
 * 
 * Lets fix ordering of months first. Not an easy way to do this sadly. Some suggestions we can follow: First is try month num instead as long as it is casted as an int
 */
		bad_big_dataset = big_dataset.groupBy("level").pivot("monthnum").count();
		
/* OUTPUT:
 * level | 1 | 2 | ...
 * INFO | 29302 | 28339
 * ...
 * 
 * Works but we get numerical months. Not good enough but it is in order. Again, no simple way to get around this. What we can do on pivot method is to use an alternative method of pivot method
 * This takes a list of objects and these are values we expect for pivot column. We are looking because we know the order of values, Jan, Feb, March, etc. If we are lucky in a situation like this, 
 * we can populate a list of values and this tells Spark which values we want to show in the table with their orders. However, without this value, behind the scenes Spark basically has to do a query
 * to see what values are present in month num before building pivot table. SO these are two separate and potentially expensive operations. HOWEVER by telling Spark the values that will
 * be present, Spark will not have to do this and it can go ahead and build the table.
 */
		List<Object> bad_columns = new ArrayList<Object>(); //first lets try to make a new array list with objects
		bad_columns.add("March"); //march will be passed in as a string. Output will work but this will be annoying to keep writing columns.add
		
		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}; //inline array, much easier
		List<Object> columns = Arrays.asList(months); //pass in the list to the list of objects
		
		big_dataset = big_dataset.groupBy("level").pivot("month", columns).count(); //pass in columns list, pivot on the month. Output works
		
/* If we know the columns in advance, recommended to do the long winded approach with Object[] months. 
 * One thing we almost forgot about: THe table we have has ecery cell with a value which is lucky. What if there was a 0 for any of the cells? We could technically edit the file
 * and we can add an extra month for matching values of a col full of 0s. No matching values for a col that doesnt exist will be a series of NULLs rather than series of 0s.
 * Spark doesnt know what to do. Lets tell it what to do. There is a method called .na() for not available. This will give us an intermeidate obj we can call.
 * We can choose what to do in the event of blank cell. Common option is fill method to fill blanks with whichever value we choose like 0.
 * .drop() will drop ROWS containing NULLs. Other options we can follow on the documentation. 
 */
		big_dataset = big_dataset.groupBy("level").pivot("month", columns).count().na().fill(0); //for not available fcn and fill it with 0s.
		
		
		big_dataset.show(100); 
		
		spark.close();
	
	}
}
