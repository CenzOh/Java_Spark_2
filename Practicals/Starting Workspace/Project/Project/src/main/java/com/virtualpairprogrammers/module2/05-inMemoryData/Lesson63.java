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


public class Lesson63 {
	
/* Lesson 63 In Memory Data
 * Recall that we used the parallelize method to initialize a java RDD with basic in-memory data that was seeded from a list. When working up a query, may be useful to have
 * a set of hardcoded data in memory data like what we did with RDD. Thats how we could make up a unit test like J Query. Lets look at a semi realistic dataset.
 * Want to be familiar with more common scenarios like pulling in data from big text files, data dumps, log files. Remember in RDD section we made a regular java list, this was
 * an in-memory list and we added some data to it to set up initial RDD> lets see how to do it in Spark SQL. Advantage of this over reading in a text file is that we can run
 * unit tests.
 * List inMemory = new ArrayList(); //version in RDDs.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
//		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");
		
		List<Row> inMemory = new ArrayList<Row>(); //import list and arrayList from java.util
		
//		dataset.createOrReplaceTempView("my_students_table");
//		Dataset<Row> results5 = spark.sql("select distinct(year) from my_students_table order by year"); //ascending order by set of years. If want desc just add that at the end of year
//		results5.show();

/* next we want to create a spark SQL row in-memory without reading in a file, use the class rowFactory, use .create() method. Takes a variable number of parameters (as many) any type of object
 * We can create multiple columns on the fly! Lets use our log example for this. We will have two columns, one is log level like WARN, FATAL. Second is a date and time.
 */
//		inMemory.add(RowFactory.create("WARN", "16 December 2018") ); //returns object of type Row. Can add this row to our in-memory list
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32") );
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34") );
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21") );
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21") );
		inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20") );
		
/* DataFrame is a data set of rows. Technical difference between the two which we will look at later on. THere use to be a class called a DataFrame in the API but for some reason it was removed
 * and now the DataSet of rows is the same as the old DataFrame class. SO, we can use the .createDataFrame() method to create a DataSet of rows.
 */
		
//first structfield object represents the first of our columns which is log level and type string. Pass in name, dataType, bool if its nullable, metadata
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty() ),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		}; //this is an in-memory structure, spark can understand title of each col and data types. Make metadata blank by calling it with .empty() function
		
		StructType schema = new StructType(fields); //create schema with new instance of struct type. The struct type class is just a wrapper, needs to be initialized. CTRL SPACE to see what we need
		Dataset<Row> dataset = spark.createDataFrame(inMemory,  schema); //we will call this by passing an in-memory list, then pass in an object of type struct type to represent a schema
		
/* WE know the above is a dataset of rows. WIll call it dataset for now.
 * What is a schema? It is an object telling Spark what the data types are for each column. As simple as that. Creating it is a bit long winded.
 * Run the program we get:
  level | datetime
   WARN | 16 December 2018
   
   Good it works. Again we will be having an in-memory collection of rows that we then used to initialize our data set to run code quick and have a basic dataset to work from.
   Refer to the extras file with a text file of in memory data we will use to add to in-memory data structure
 */
		dataset.show(); //should run much faster


//		spark.close(); //close method on spark session unsure why not compiling
	
	}
}
