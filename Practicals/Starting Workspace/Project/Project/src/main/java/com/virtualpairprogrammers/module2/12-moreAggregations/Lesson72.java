package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class Lesson72 {
	
/* Lesson 72 How to use agg method in Spark
 * So far we used Spark to crunch down a big data set and present it in the form of this handy pivot table, we have not talked about aggregation functions in general.
 * We have done counting, nothing numerical really. Lets look at operations like how to find max value in a certain column or an average or standard deviation (std).
 * We will move on from the logging example. Only used it to demonstrate pivot tables. We will switch back to the student exam result data which will assist better with these calculations.
 * Instructor creates a file called ExamResults.java but I will just write everything into this file. Everything we have been doing so far would have been in Main.java.
 * 
 * For our data, can do a dataset.show() so we can see what we are working with. Recall, every row represents an exam results. A particular student may do multiple exams. The exam results
 * are spread across different exam centers (may not use this col). Each exam has a subject, a year and a quarter. This video we will work on the score. FOr each subject, what was
 * the highest score? DOesnt matter the year. Gather all results for Math. This should be straight forward. 
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); //changed source file
		
/* We will be working with the java api. groupBy() will gather all the rows with the SAME subject. THen we perform an aggregation. Look for maximum score. Will be applied to each result.
 * If we type . after the groupBy() we can see methods other than .count() like .mean(), .max(), .min(), etc. Although these methods are just convenience methods.
 * So if we call the .max() method, it is NOT very flexible. First issue - specify the col name to perform max on which is score col. This should work and it will return a new dataset (one row
 * for each dataset). If we try, this will not compile. Remember all the cols are treated as string. Exception says score is NOT a numeric col. COnvert the scores to numeric values
 */
		
		dataset = dataset.groupBy("subject").max("score"); //doesnt work because spark reads the score col as a string.
		
/* One fix: when spark reads in initial CSV, tell it to read the cols as specific datatypes. Can do this after spark.read() with the .option() method like how we used that to specify
 * headers. In spark documentation, under csv options we can see all methods for .option(). The one we are interested in is inferSchema. THis works out what the schema / data type for 
 * each col automatically from data. HOWEVER it does require one extra pass over the data. For big data, this can be an expensive operation. Instructor not very keen on this idea. Is
 * a possibility and easily can set. Just write:
 * 
 * .option("inferSchema", true)
 * 
 * Will run and not crash but keep an eye on the performance. Lets look at a manual cast of the data type since it is better for performance. We will see that the obv way to do this
 * will NOT work because .max() method is NOT flexible enough to accomodate a cast. Recall, if we obtain the corresponding column object for the score we can call the cast method on it:
 */
		Column score = dataset.col("score"); //grabbing score col, import the spark.sql one
//		dataset = dataset.groupBy("subject").max(score.cast(DataTypes.IntegerType) ); //instead of passing in a string, pass in col obj itself. Call .cast() method as an integer
		
//		dataset = dataset.groupBy("subject").max(functions.col("score") ); //can use functions class to call col without the static import
		
//		dataset = dataset.groupBy("subject").max(col("score") ); //same thing but w/o functions method since we added static import
		
//		dataset = dataset.groupBy("subject").max(col("score").cast(DataTypes.IntegerType) ); //doesnt work either if we cast that col as an integer :(
		
/* Strange thing is .max() fcn will NOT compile with column objects we can only use string here which means we cant cast it to the right data type. Instructor is not sure of this.
 * There is a general purpose method in the API that we can use. Recall the .groupBy() returns an object where we get the .max() method. Recall this is NOT a dataset (the .groupBy() ) is of type
 * RelationalGroupedDataset. If you look at that class in java doc, the class does hold set methods for aggregations. But main method is the .agg() fcn which has multiple variants.
 * 
 * That is purpose of this video, what is .agg() fcn? In the doc scroll down and you can see the .agg() methods which shows it can be used to perform ANY of the supported aggregation
 * fcns in spark. SO by using the .agg() method we can do .mean(), .max(), .count(), etc. We use the .agg() method to do all our aggregations and the other methods are just
 * convenience methods that sort of call .agg() under the hood. Not sure why the simpler form d/n let you pass in columns. But .agg() method will work for us.
 */
		
		dataset = dataset.groupBy("subject").agg(functions.max(col("score").cast(DataTypes.IntegerType))); //call .agg() fcn, inside use functions .max(), specify col, cast as integer. Compiles!
/* subject | max(CAST(score AS INT))
 * Philosophy | 98
 * Math | 98
 * ...
 * 
 * This works which is great. However, looks like the max attainable score is 98 not 99 or 100. Interesting. Lets add an alias
 */
		
		dataset = dataset.groupBy("subject").agg(functions.max(col("score").cast(DataTypes.IntegerType) ).alias("max score") ); //adding an alias
	
/* The .agg() method is the MOST flexible way of performing aggregations we may want to use one of the more basic aggregation methods for basic requirements otherwise can use this version.
 * Super cool thing about this .agg() method is how flexible it is. We can perform multiple aggregations at the same time. Lets demonstrate with max and min score:
 */
		
		dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType) ).alias("max score"),
												 min(col("score").cast(DataTypes.IntegerType) ).alias("min score") );
		
/* subject | max score | min score
 * Philosophy | 98 | 0
 * Math | 98 | 0
 * ...
 * 
 * Looks like it will be correct. Note - original reason we used the .agg() fcn was because of casting issue. THe data type problem, we used the .agg() method
 * to call the .cast() method on each col but we went further to see how it can be used with multi aggregations. One odd thing though, a quirk of the API:
 * When using cols version in .agg() method, if you dont bother doing a cast it will auto do the cast. Interesting.
 */
		
		dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score"),
											 	 min(col("score")).alias("min score") ); //this will compile

		dataset.show(100); 
		
		spark.close();
	
	}
}
