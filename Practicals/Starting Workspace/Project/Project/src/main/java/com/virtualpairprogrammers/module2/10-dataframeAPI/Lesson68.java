package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()


public class Lesson68 {
	
/* Lesson 68 SQL vs DataFrames
 * We will look at the Java API in SQL. We can call this the DataFrame API. Terminology a bit confusing. We have two different programming models within Spark SQL. So far we have
 * been doing SQL style syntax called Spark SQL. There is a pure java API to achieve the same thing and we can go further with it. Commonly called DataFrame API. SOmetimes called
 * DataSet API. Confusion is DataFrames is a synonym / alternative for a DataSet of ROWS. Java API also now supports DataSets of regular Java objects. Can get a data set
 * of custom java object like Customers. If we talk about DataFrames then we talk about DataSets of Rows. We can have DataSets of anything tho. Traditional name is DataFrame.  
 * Again of note, SQL syntax good for focusing on Data Science part. If want more coding part, use the Java API / DataFrame API instead. Lets build the SQL statement we have previously
 * into Java API
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
		
/* Raw data set:
 * level | datetime
 * DEBUG | 2015-2-6 16:...
 */
		Dataset<Row> big_dataset1 = big_dataset.select("level"); //select just the level. THis is a heavily overloaded method

/*
 *  level
 *  DEBUG
 */
//		big_dataset.select("level", "date_format(datetime, 'MMMM') "); //we can specify expressions like in SQL. Doesnt quite work. Exception is can not resolve because of input col type
//select() method expects the strings to be column names
		
		Dataset<Row> big_dataset2 = big_dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month"); //selectExpression will work and alias works too
		
/* level | month
 * DEBUG | Febuary
 * ...
 * 
 * But it looks like SQL and is easiet way to call the fcn but some people dont like it so lets revert it back to raw select. Recall functions() class.
 */
		
//		Dataset<Row> big_dataset3 = big_dataset.select("level", functions.date_format(arg0, arg1)); //with our static import ...functions().*; allows us to not have to explicitly call functions
		
//		Dataset<Row> big_dataset4 = big_dataset.select("level", date_format("datetime", "MMMM")); //getting an error, not being recognized as string format? 
//Remember the col() method we can pass in name of col returns string
		
//		Dataset<Row> big_dataset5 = big_dataset.select("level", date_format(col("datetime"), "MMMM")); //feels more java like. Compiler error at select. Calling date_format method returns obj of type col
//the select method allows us to use strings or col objects. Cant mix and match. Have to be consistent. Now lets make the other cols prefixed with col()
		
		Dataset<Row> big_dataset6 = big_dataset.select(col("level"), date_format(col("datetime"), "MMMM")); //now it compiles. Instructor thinks this is a bit clunky and would rather use selectExpr.
//but again, up to us what way we want to do this.
		
/* level | date_format(datetime, MMMM)
 * DEBUG | February
 * ...
 * 
 * How do we add an alias to that second column? for any col object (entire expression of date_format), can add the . between two closing brackets, call the as method. There is an alias method
 * which does the same thing
 */
		big_dataset = big_dataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month") );
		
/* level | month
 * DEBUG | February
 * ...
 * 
 * Great will continue next lesson.
 */
		
		big_dataset.show(100); 
		
		spark.close();
	
	}
}
