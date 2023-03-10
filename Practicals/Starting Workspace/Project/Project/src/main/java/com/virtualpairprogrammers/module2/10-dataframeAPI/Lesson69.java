package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class Lesson69 {
	
/* Lesson 69 DataFrame grouping
 * This lesson we will see how to do groups. A little more intuitive to do with the Java API. We can call the .groupBy() method directly on the dataset. Takes col reference object or 
 * use raw strings. In our example we will group by level and month. Complexity here is that this will NOT return a data set. If you try to assign it you will get mismatch.
 * Mismatch error is can not convert from relational groups data sets to a data set of rows. It just like in SQL, when we perform a grouping, must perform an aggregation on all the cols
 * NOT a part of grouping. (refer to previous lectures about how grouping works if dont understand).
 * 
 * Look at documentation of the class RelationalGroupedDataset to see more about it. THis class holds all the aggregation methods that we perform after doing a group by. We will do pivots
 * next section of course. We are interested in groupBys. There are usual aggregated methods like count(), max(), sum(). We want to count() the rows found for each group. Just do .count()
 * on the object. This method will return back the dataset so then we can assign it to our dataset.
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

		Dataset<Row> bad_big_dataset = big_dataset.select(col("level"), 
										date_format(col("datetime"), "MMMM").alias("month"), 
										date_format(col("datetime"), "M").alias("monthnum") ); //updated to include numerical month so we can order by numerical month
		bad_big_dataset = big_dataset.groupBy(col("level"), col("month")).count(); //can call groupby method. group by level AND month. do .count() method to return back dataset
	
/* OUTPUT:
 * level | month | Count
 * WARN | June | 8191
 * INFO | June | 29143
 * ...
 * 
 * Great now all we have to do is get the dataset correctly ordered like we did with SQL. 
 */
		bad_big_dataset = big_dataset.orderBy("monthnum"); //orderBY() takes one or more column references. We want to order by the month in numerical form

/* OUTPUT: Exception error. Reason is it can not resolve month num given input cols are level, month, count. We did have a month num here before group by. Same root cause that we saw earlier
 * in SQL version. SO think about it, we are selecting level and month. We also wanted to include monthnum but month num is a group of values:
 * 
 * Level | month | count(1) | monthnum
 * WARN | DECEMBER | 2 | [12,12]
 * 
 * Spark SQL was saying it doesnt know what we want it to do with the big group of values. Advise is, any col not part of the grouping must have an aggregation function performed on it.
 * Same thing here. But exception doesnt tell us that. When we do the group, anything that is not part of the group is discarded from the dataset. SO the dataset we have here
 * doesnt have month num anymore. We fixed this in SQL was adding a call to first aggregation function. Now think about it, the month numbers will always match. If January, always be 1.
 * what if we add month num to group clause? We are really grouping by the month and month num. By doing this monthnum will persist in the final dataset.
 */
		
		Dataset<Row> bad_big_dataset1 = big_dataset.select(col("level"), 
				date_format(col("datetime"), "MMMM").alias("month"), 
				date_format(col("datetime"), "M").alias("monthnum") );
		
		bad_big_dataset1 = big_dataset.groupBy(col("level"), col("month"), col("monthnum")).count(); //updated to include monthnum in the dataset. 

		bad_big_dataset1 = big_dataset.orderBy("monthnum"); //make sure to assign dataset to a new object.
		
/*OUTPUT:
 * level | month | monthnum | Count
 * INFO | January | 1 | 29119
 * WARN | OCtober | 1 | 8226
 * ...
 * 
 * Previous videos we saw why we received this issue. Spark assumes monthnum is an alphabetic string rather than an int. Not correct order lets fix it by passing in .cast() method
 */
		
		Dataset<Row> bad_big_dataset2 = big_dataset.select(col("level"), 
				date_format(col("datetime"), "MMMM").alias("month"), 
				date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) ); //added cast to make monthnum an int. Can also do .cast("int") instead of java objs.
		
		bad_big_dataset2= big_dataset.groupBy(col("level"), col("month"), col("monthnum")).count();

		bad_big_dataset2 = big_dataset.orderBy(col("monthnum") ); //wrapping monthnum around col() 
		
/*OUTPUT:
 * level | month | monthnum | Count
 * INFO | January | 1 | 29119
 * INFO | February | 2 | 28983
 * ...
 * 
 * Much better. Final polishing is to get rid of the monthnum col. Use the .drop() method.
*/		
		
		bad_big_dataset2 = bad_big_dataset2.drop(col("monthnum")); //drop monthnum. We can wrap them with col() or just enter them as strings. Make sure to reassign to dataset object.
//we also did a secondary sort for log levels. Easy fix!! Just add in the second sort to the orderby
		
		bad_big_dataset2 = big_dataset.orderBy(col("monthnum"), col("level") );//sorting by log level as well.
		
//again, up to us about which version we prefer. Instructor thinks SQL version is easier to read and has experience with SQL (like myself). However he thinks the java api is better for
//a more complex report. Its fine to switch between the two. Can use the SQL to get a data set of rows then use further java calls and convert it into a temp view and do ANOTHE SQL against it.
// next time we will convert this into a pivot table.
		
		
		
		big_dataset.show(100); 
		
		spark.close();
	
	}
}
