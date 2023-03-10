package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()


public class Lesson73 {
	
/* Lesson 73 Building a pivot table with multiple aggregations
 * Last lecture we used some spark SQL API. We will have small exercise now. The requirement is we are using the student.csv. We are looking for a pivot table which will show each subject for the
 * exams down the left hand side. Each year will be across the top of the table. For every cell in the table, we want to see the avg exam score for that subject. Do this with std dev.
 * Represents spread of values. Make them come back in two decimal places. Can also use .round() function.
 * 
 * I think it should look like this:
 * Subject | 2014 | 2015
 * Math | 86.22 | 89.55
 * ...
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); 
		
// ***** MY ATTEMPT ***** 
		dataset = dataset
						 .groupBy("subject") //first we group by the subject / no subject is duplicated. Left side
						 .pivot("year") //expand the years across the top / each year its own column
						 .agg(functions.avg(col("score"))); //call aggregate function, calculate the average based on score column. Around 55? Also not good
//						 .round("year", 2); //unfamiliar with using .round() method in spark.sql. Tried looking at doc, cant figure it out not sure why not compiling. I think this is format
		
//						 .agg(functions.stddev(col("score"))); //tried to do std dev like what instructor said. was around 19? Not good
						
//						 .select(col("subject"),
//								 col("year")) //at first I used this but quickly realized i cant grab the score if i only select subject and year
		
// ***** INSTRUCTOR ANSWER ***** 
/* Subject will be the first col we perform the grouping on since it should be on left hand side... Oops I forgot we wanted avg AND std dev. Use .agg() method for MULTIPLE aggs
 * avg method and .mean() method are the same. Pass .round() method INSIDE the aggregate col. Including two aggregates, what will output look like? Spark is clever enough to put each
 * aggregate in its own col. 2014 avg | 2014 std dev | 2015 avg ... Lets fix column header with .alias()
 */
		dataset = dataset.groupBy("subject").pivot("year").agg( round( avg(col("score")), 2 ).alias("average"), 
																round(stddev(col("score")), 2).alias("stddev") );
		
// ***** MY VERSION REVISED ***** 
		dataset = dataset
				 .groupBy("subject")
				 .pivot("year")
				 .agg( round( avg( col("score")	), 2).alias("average"),
					   round( stddev( col("score") ), 2).alias("stddev") );
	

		dataset.show(100); 
		
		spark.close();
	
	}
}
