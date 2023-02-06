package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Lesson70 {
	
/* Lesson 70 How does a pivot table work?
 * Will show to how to make one in Spark SQL. Previously we made the report from a million logging records. Looks okay because hard to see any underlying patterns. Layout is not good.
 * Lets lay the table out better. Pivot tables are common in spread sheets like Microsoft Excel. Some databases support pivot tables with non standard SQL syntax such as Oracle DB.
 * Pivot table is a different way to lay out a table that we already have. We can use it when we have two groupings and an aggregation (like what we have). 
 * Instead of laying out each individual grouping as a long series of rows, instead we can set the values for one of the groupings (lets say month) and make the months become columns.
 *
 * Ex - Flat Table
 * Level | Month | total
 * INFO | APRIL | 1342
 *...
 *
 * Every one of the rows is a distinct pair of level and month, together with an aggregation which was a count of how many of that combination there was. Key thing is, there are two columns
 * here that are formed parts of the group. Now lets say we want to leave the level column on the left hand side of the table. But each distinct value in the month column we will make
 * it its own column. 
 * 
 * level | April | May | December
 * INFO | ...
 * FATAL | ...
 * WARN | ...
 * 
 * Again for one of the groupings (level) leave it as before. The difference is that now we only need three rows for each level. In the flat table there are only three distinct examples.
 * We had FATAL twice for different months. Now we have the distinct months spread across the table so we dont need duplicate lines for level anymore. The table will be much more compact.
 * Again, pivot tables are useful when we have two groupings. We can call the left side of the pivot table row grouping and the top side the column grouping. 
 * 
 * With any other grouping, we will perform an aggregation and the values that will be filling in for us will be the count like we had before. So again the pivot table just reorders the table.
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
		
		Dataset<Row> bad_big_dataset2 = big_dataset.select(col("level"), 
				date_format(col("datetime"), "MMMM").alias("month"), 
				date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) ); //added cast to make monthnum an int. Can also do .cast("int") instead of java objs.
		
		bad_big_dataset2= big_dataset.groupBy(col("level"), col("month"), col("monthnum")).count();

		bad_big_dataset2 = big_dataset.orderBy(col("monthnum") ); //wrapping monthnum around col() 
	
		
		bad_big_dataset2 = bad_big_dataset2.drop(col("monthnum")); //drop monthnum. We can wrap them with col() or just enter them as strings. Make sure to reassign to dataset object.
		
		bad_big_dataset2 = big_dataset.orderBy(col("monthnum"), col("level") );//sorting by log level as well.
		
		
		bad_big_dataset2.show(100); 
		
		spark.close();
	
	}
}
