package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson60 {
	
/* Lesson 60 Filters and Lambdas
 * WE have some alternatives to do a filter in spark SQL. Instructor thinks what we have done is preferred way while other methods not as keen will look at them anyway
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");

//		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' ");
//		modernArtResults.show(); 

//		Dataset<Row> modernArtResults2 = dataset.filter("subject = 'Modern Art' AND year >= 2007");
//		modernArtResults2.show();
		
/* First alternative is to do something similar to what we did with RDDs. Recall most common way is to pass in the lambda expression. Each element of the lambda expression was a single string
 * or a tuple. DIfference is that Spark SQL contains a series of rows. TO use lambda syntax for filter, each element we are given is of type row. Other than that, same as in spark RDD. Include
 * an expression where we evaluate a particular row and if we return true, the row will be included in the results. Lets do logic of commented out line.
 */
//		Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art") );
// for a row we are evaluating check subject col and see if it equals Modern Art. If yes include in resulting dataset. These two lines are pretty much the same no performance difference
// lets also include when year is greater than 2007. Prints same results
		
		Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art")
															&& Integer.parseInt(row.getAs("year")) >= 2007);

		modernArtResults.show();		
		
//		spark.close(); //close method on spark session unsure why not compiling
	
	}
}
