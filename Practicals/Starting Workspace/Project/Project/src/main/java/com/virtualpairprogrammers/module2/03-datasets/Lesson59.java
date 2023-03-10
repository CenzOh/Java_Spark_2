package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson59 {
	
/* Lesson 59 Filters using Expressions
 * LAst time we looked at basics of datasets which are basically a collection of rows. We also did basics of a row by extracting the first row and looking at some values.
 * However this is not very realistic. We need to do operations to filter down to the data we are interested in or aggregations across entire dataset.
 * Lets say we want to filter this dataset so we can end up with a dataset where all exam results are for Modern Art. There may not be a lot in first 20 rows.
 * Look below on how to do this:
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");

//		dataset.show();

//		long numberOfRows = dataset.count(); 
//		System.out.println("There are " + numberOfRows + " records");
		
//		Row firstRow = dataset.first(); //takes first row of dataset. 
		
//		String subject = firstRow.getAs("subject").toString();
//		System.out.println(subject); //Math. This is what we expected to see!
	
//		int year = Integer.parseInt(firstRow.getAs("year")); //2005. So now it works and converts it into an int instead of being a string.
//		System.out.println("The year was " + year); //so overall be mindful about data type conversion. Everything comes back as raw string when using CSV files
		
/* First, note that the dataset object has a .filter() method available. Just like in an RDD. Difficulty is that there are 4 versions with different parameters. One version can take in a string
 * another can take a condition expression, another can take a function (lambda expression). There are two remaining options which are basically the same thing. We have three different ways
 * to perform a filter in Spark SQL. Common theme is that we have multiple ways to achieve the same thing. Lets start with the simplest where the method takes in a String called
 * condition expression, simply takes a string. Use what you would see in a where clause in SQL. We want to find where subject = modern art. No need for keyword, just write expression.
 * 
 * This filter method returns a new data set with just the filtered rows. What is happening here is the SAME thing as previous sections! Recall that datasets like RDDs are immutable.
 * No methods to modify the dataset in memory. Always will get new datasets returned as a result. What we do is exactly as before under the hood, an execution plan is being built up
 * and only at the point when we come to an operation will spark need to execute that execution plan and data will then be read in on a real cluster.
 */
		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' "); //a string inside single quotation marks
		
		modernArtResults.show(); //this will cause data to be shuffled around etc, execution plan will start at this point because this is an action. Prior line sets up execution plan.
		
/* student_id | Exam_center_id |  subject | year | quarter | Score | grade
 * 			1 | 			 1 |Modern Art| 2005 | 		 1 |    32 |     E
 * 			2 | 			 1 |Modern Art| 2005 | 		 1 |    55 |     C
 * ... subset of output. It works tho! We may have guessed that we can chain our conditions together. Ex - we want all modern art subjects BUT from year 2007 and onwards. We
 * are getting results from 2005
 */
		Dataset<Row> modernArtResults2 = dataset.filter("subject = 'Modern Art' AND year >= 2007"); //use AND clause for multi conditions
		modernArtResults2.show();
		
//it works, student IDs start around 42,000. So it had to go in deep the dataset to find results.
		
		
//		spark.close(); //close method on spark session unsure why not compilin
	
	}
}
