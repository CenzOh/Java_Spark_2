package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson62 {
	
/* Lesson 62 Using a Spark Temporary View for SQL
 * We will be looking at FULL SQL syntax in spark SQL. We can use a java API (will look at later). The SQL API is better if we want to focus on the data science rather than coding.
 * We looked at writing filters, there are several approaches to writing filters. Two java-ish ways, either domain specific language (pretty readable but complicated) or lambdas
 * (not as readable). More common way is to use SQL syntax. WE will look at it in more detail. 
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");

// sql
//		Dataset<Row> modernArtResults2 = dataset.filter("subject = 'Modern Art' AND year >= 2007");
		
// lambda
//		Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art")
//															&& Integer.parseInt(row.getAs("year")) >= 2007);

// functions class
//		Dataset<Row> modernArtResults1 = dataset.filter(col("subject").equalTo( "Modern Art" )
//				  													  .and(col("year").geq(2007)) );
		
		dataset.createOrReplaceTempView("my_students_table");
		
/* If we call this view students, we can refer to that view in a later SQL statement. We can call it whatever we like! In spark can think of a view like a table in a relational databse.
 * Note this method does not return anything. No reference to the view idea. This is because NOW back to the original spark session object there is a method called .sql() it allows us to
 * execute any arbitrary SQL statements against any of the views created. We can do anything we want, but first we will do a filter
 */
		Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'French' "); //returns a dataset of rows
		results.show();

/* student_id | Exam_center_id | subject | year | quarter | Score | grade
 * 			3 | 			 1 |  French | 2005 | 		 1 |    53 |     C
 * 			4 | 			 1 |  French | 2005 | 		 1 |    21 |     E
 * 
 * It works although it is similar to what we have done previously, using filters. Point is we have full expressiveness of SQL available to us. If we are interested in one of the columns
 * we can easily do projecting which just selects a subset of the columns in a dataset. Or if we wanted to take the score from each of the rows, we can select whichever columns with , separator.
 */
		Dataset<Row> results1 = spark.sql("select score, year from my_students_table where subject = 'French' "); //ultimately we are sitting on an RDD.
		results1.show();
		
/* score | year
 *    53 | 2005
 *    21 | 2005
 */
		
		//lets try another one, find max score across all exams for french. Call max function.
		Dataset<Row> results2 = spark.sql("Select max(score) from my_students_table where subject = 'French' ");
		results2.show();
		
/* max(score)
 *         98
 */
		
		//we can do avg for average and it knows it wont be whole num so it auto converts scores to double
		Dataset<Row> results3 = spark.sql("select avg(score) from my_students_table where subject = 'French' ");
		results3.show();
		
/* avg(CAST(score AS DOUBLE)
 *          55.7156320216328
 */
		
		//in which year have we been tracking results for this college? Lets select the year, take out where clause so we can see it.
//		Dataset<Row> results4 = spark.sql("select year from my_students_table "); //this one not good because we got duplicate years
//		Dataset<Row> results4 = spark.sql("select distinct(year) from my_students_table "); //distinct() gathers all identical years and bundles them into one row, much better
		Dataset<Row> results4 = spark.sql("select distinct(year) from my_students_table order by year"); //previous was in random order, use order by to put in order
		results4.show();
		
/* year
 * 2005
 * 2006
 */


//		spark.close(); //close method on spark session unsure why not compiling
	
	}
}
