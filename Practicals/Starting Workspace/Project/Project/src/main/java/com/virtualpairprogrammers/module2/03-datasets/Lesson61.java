package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*; //use this instead for .col()

public class Lesson61 {
	
/* Lesson 61 Filters using COlumns
 * We have seen two ways to do the filter. SQL way is most natural way honestly. Second is more traditional lambda way. We will get rid of lambda version. Lets look at a third approach
 * which is a bit more complex but it is a programming style we use a lot throughout rest of course. More programmatic approach to do the filter.
 * If we had something more dynamic, add some AND, OR clauses on a particular logic, will be hard with the lambda approach, nasty string concatenation.
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
//		Dataset<Row> modernArtResults2 = dataset.filter("subject = 'Modern ARt' AND year >= 2007");
		
// lambda
//		Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art")
//															&& Integer.parseInt(row.getAs("year")) >= 2007);

/* Third approach, more programmatic, wont be concatenating a string. Revolves around the col class. Havent seen it yet. This class represents a col within our datasets.
 * Simply call the .col() method on our dataset and type in which col name. Also be careful with the import, there are four different column classes in spark API. We want the one
 * org.apache.spark.sql() We can use this object in an expression
 */
		Column subjectColumn = dataset.col("subject");
		
/* simply here just pass in the column object into the expression. Can also type . to perform expressions against the column. .leq() means less than or equal to. .lt() is less than
 * .gt() greater than, .geq() greater than equal to, .like() is a wild card, to replicate what we have use .equalTo() DONT use .equals(), thats the regular java equals method which will
 * compare the column object itself. 
 */
		
//		Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo( "Modern Art" ); //checking if it runs, it does and works. To get year just make a new column object
		
		Column yearColumn = dataset.col("year");		
				
		Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo( "Modern Art" )
																	.and(yearColumn.geq(2007)) ); //and year col greater than equal to 2007, does work
		modernArtResults.show();
		
/* This style is like standard straight forward java, good to use if want to programmatically use some logic like adding the AND, OR clause. Good for building up potentially
 * complex and difficult to understand string. ACtually, most spark SQL programmers would have NOT written the expression like this! Slightly messy to write two lines 
 * to get the column objects. There is a class in the spark SQL API called functions. Note that the class has lowercase first letter `functions` unusual for Java. This class contains
 * a long series of static methods. We can call these methods which are more like functions that we can call without creating an instance of an object first. Ex - functions.asc(). THey
 * all return an instance of a column. THere is a method called column() and returns the column based on a column name. Lots of duplication, theres even a version just called .col()
 * SO instead of calling .col() on the dataset, we could have done functions.col()
 */
		Column subjectColumn1 = functions.col("subject"); //better version below for just .col()
		Column yearColumn1 = functions.col("year");
		
/* FOr the import, make it static and add a .* at the end. This means all the static methods inside the functions class can be called WITHOUT prefixing it with functions. Instead directly call
 * .col() It knows this from the import that there is a static method of static function inside the class
 * import static org.apache.spark.sql.functions.*;
 */
		Column subjectColumn2 = col("subject");
		Column yearColumn2 = col("year");
		
/* Note we could have done this directly in the expression instead of having these two lines of code. So those two lines of code are redundant. The creators of the API are aiming for
 * a domain specific language. THis means they are aiming to make the code as readable in the way we would read an English statement. We can use the full word .column() there, most
 * spark SQL programmers use shortened version
 */
		Dataset<Row> modernArtResults1 = dataset.filter(col("subject").equalTo( "Modern Art" )
																	  .and(col("year").geq(2007)) );
		
		modernArtResults1.show();		
		
//		spark.close(); //close method on spark session unsure why not compiling
	
	}
}
