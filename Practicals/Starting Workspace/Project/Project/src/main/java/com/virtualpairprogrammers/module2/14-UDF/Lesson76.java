package com.virtualpairprogrammers;

import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;


public class Lesson76 {
	
/* Lesson 76 Using a UDF in Spark SQL
 * Back to the logging example to show how this works with spark SQL. Next video about performance. Check if everything runs right. Recall we worked hard to get the month sorted correctly. 
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/biglog.txt"); //back to the logging example 
	
// OLD SQL VERSION
		
//		dataset.createOrReplaceTempView("logging_table");
//		Dataset<Row> results = spark.sql
//		("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
//				+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); 
		
// JAVA API VERSION
//		dataset = dataset.select(col("level"),
//								 date_format(col("datetime"), "MMMM").alias("month"),
//								 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
//		
//		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
//		List<Object> columns = Arrays.asList(months);
//		
//		dataset = dataset.groupBy("level").pivot("month", columns).count(); 
		
//UDF SQL VERSION
//idea is to convert a month from proper month jan feb etc into an int like 1 2 3. Reminder, first param is name of fcn. Second param is implementation of fcn. THird param is return type
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM"); //input month is full name of month like January
		SimpleDateFormat output = new SimpleDateFormat("M"); //using java api to say if input is jan return 1. Again output will become a number
		
		spark.udf().register("monthNum", (String month) -> {
			
			java.util.Date inputDate = input.parse(month); //call parse method to turn string coming in into a regular java date object. java.util.Date
			
			Integer.parseInt(output.format(inputDate) ); //pass it through output formatter
			
		}, DataTypes.IntegerType);
		
		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> results = spark.sql
		("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
				+ "from logging_table group by level, month order by monthNum(month), level"); 	//in order by we can order by monthNum!
		
/* Interesting thing is in the documentation or examples online, they dont use the lambda syntax at time of recording. If you look at the examResults.java you can see how it does not use
 * old fashioned syntax. I will demonstrate below what the old fashioned syntax will look like but first the newer style:
 * 
 * 		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); 

		
		spark.udf().register("hasPassed", (String grade) -> {
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"); 
			
		},  DataTypes.BooleanType);
		

		spark.udf().register("hasPassed", (String grade, String subject) -> {
			
			if (subject.contentEquals("Biology")) { 
				
				if (grade.startsWith("A")) return true; 
				return false; 
			
			} 
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"); 
			
		},  DataTypes.BooleanType);
		
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject") ));
 */
		
		
//		dataset.show(100); 
		results.show(100);
		
		spark.close();
	
	}
	
//Old syntax for ExamResults:
	public static UDF2<String, String, Boolean> hasPassedFunction = new UDF2<String, String, Boolean> () {
		
		@Override
		public Boolean call(String grade, String subject) throws Exception {
			
			if (subject.contentEquals("Biology")){
				
				if(grade.startsWith("A")) return true;
				return false;
			}
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}
	};
/* This is old fashioned java syntax and is anonoymous in a class and we would have used this in Java 1 - 7 to do a lambda fcn. Important detail - to create an instance of this class
 * called UDF followed by a number, this defines the amount of parameters we want to accept. In our example we take the grade and subject (two parameters).
 * This is the same functionally. Recommended to FAVOR JAVA 8 LAMBDAS.
 */
}
