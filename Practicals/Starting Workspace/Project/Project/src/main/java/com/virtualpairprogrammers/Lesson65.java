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


public class Lesson65 {
	
/* Lesson 65 Date Formatting
 * In this lesson we will build upon our basic report about grouping and aggregation. Lets make it more meaningful by converting raw date and time input data into something more useful.
 * Lets set a requirement: FATAL November 9382. This means for the month of November, we want to see 9382 FATALs. This accounts for all years in the data, 2016, 2017, etc. Same with other
 * combinations as well:
 * FATAL January 27. WARNING December 32.
 * Lets see how to get this. FIrst, see that we have raw data in our logs whereas we want to work on months and have the full month name.
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
		
		dataset.createOrReplaceTempView("logging_table"); 
		
//		Dataset<Row> results = spark.sql("select level, datetime from logging_table"); //take out the groups for now. Keep it basic. In second col we want the month
		
/* In Spark documentation there is a function called .date_format() which takes a time stamp. Spark SQL is clever enough to convert the string we have into a time stamp automatically
 * We can specify a format string. In the example they show they are not using a proper format and are able to extract the year. They pass in the string 'y' and get the year back.
 * This format string is just the standard date formatting strings we may have seen in other libraries which is standard built in Java.text API. We can call on the date time the
 * date_format. Two params, the date time from data set, and what we want. Lets try year first.
 */
		
//		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'y') from logging_table"); 
		
/*
 level | date_format(CAST(datetime AS TIMESTAMP), y)
  WARN | 2016
 FATAL | 2016
  WARN | 2016
  INFO | 2015
 ...
 
 It returns the year which is great. But may seem a bit confusing because standard way to work with these digits is, if you want 4 digit year, you write yyyy. That will work the same.
 If want two digit year, write yy. Now lets extract the month. But where do letters come from? THis is standard java from java doc text library the class is called simple date format
 on API. For month, we will use M. Yes capital M for month in year while lower case m gives minute in hour. Also with month we have the option to choose a number (07), short string (Jul), or
 a long string (July). Singel capital M is number version
 */
		
//		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'M') from logging_table"); //lower case m gives minute in hour and capital M gives month in year
		
/*
 level | date_format(CAST(datetime AS TIMESTAMP), M)
  WARN | 12
 FATAL | 12
  WARN | 12
  INFO | 4
 ...
 
 12 is December. If we did it with MM we would get a leading 0. Three MMM gives shorthand form. Four MMMM gives full word form. Take note that the column heading is a mess.
 Its the full version of the function call that we are making and its auto put in a cast to convert original string into a time stamp. We can fix this with an alias for the column using as
 keyword.
 */
		
		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table"); //adding alias to second column

/*
 level | month
  WARN | December
 FATAL | December
  WARN | December
  INFO | April
  ...
 */
		
		
		results.show(); 
		
		spark.close();
	
	}
}
