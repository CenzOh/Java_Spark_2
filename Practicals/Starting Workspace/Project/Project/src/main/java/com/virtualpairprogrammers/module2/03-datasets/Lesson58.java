package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson58 {
	
/* Lesson 58 Dataset Basics
 * In this chapter we will look at the dataset API in Spark SQL. We will look at filters first. We already did config work to make spark point at a datasource. Returns as a dataset.
 * That is the core object in SQL. show() method shows first twenty rows of the data. As we mentioned previously count is implemented as an operation against RDD under the hood.
 * So it will be the kind of maps and reduces that we have seen in the previous section.
 * 
 * Now that we are working in Spark SQL we can work in higher levels of abstraction. All of the oeprations that we can perform will be implemented as big data operations, maps, reducers,
 * filters and so on. We will work in the higher level API.
 *
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");

		dataset.show();

		long numberOfRows = dataset.count(); 
		System.out.println("There are " + numberOfRows + " records");
		
/* We will keep adding code. Lets first grab a single row from our dataset and see how to process just one row. Lots of ways to access rows but there is a method called first
 * This returns the first row of the dataset! What do we want to do specifically? The show method can help us see. Lets say we want to output the subject for the first row.
 * Two ways we can do it. Can call the row object and use the get method. It takes an integer and is the index of the col, index starts at 0. So get col 2. Now note that everything
 * in the subject col is a string so we would think we can store it as a string right? No, the get method returns a general object. THis is because if we used a datasource OTHER than 
 * a CSV file such as a JDBC to connect to databse table, then there would be different types of data like ints, dates, returned from that database.
 * We have to somehow tell java to convert to strings. Easiest way is to use the .toString() method.
 */
		Row firstRow = dataset.first(); //takes first row of dataset. 
		
//		String subject = firstRow.get(2).toString(); //grabs specific col, start at index 0, we are getting the subejct column. Use toString() to turn into string, get returns general object
		String subject = firstRow.getAs("subject").toString();
		System.out.println(subject); //Math. This is what we expected to see!
	
/* Improvement: WE have to only use the number in get() method if we DONT ahve a header row. We can instead use headers in our call. Use getAs method instead to call col header name.
 * Can seem confusing because lets say we want to extract the year and store it as an int. Note that getAs() will do auto conversion. Using int will result in runtime class cast exception.
 * THis is because the data source is a CSV. ALl the internal types in the row are type string. Auto conversion does not work very well. Lets use our usual technique to convert 
 * string to int such as Integer.parseInt() method.
 */
		int year = Integer.parseInt(firstRow.getAs("year")); //2005. So now it works and converts it into an int instead of being a string.
		System.out.println("The year was " + year); //so overall be mindful about data type conversion. Everything comes back as raw string when using CSV files
		
		
//		spark.close(); //close method on spark session unsure why not compilin
	
	}
}
