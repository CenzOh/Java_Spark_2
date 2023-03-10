package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class Lesson74 {
	
/* Lesson 74 How to use lambda to write a UDF in Spark
 * In this chapter we will look at user defined functions (UDF) in spark. How can we add columns to the dataset that are calculated from other columns? 
 * In the grade col we can see some grades are A+. Lets say we consider that a pass and we consider every other grade a fail. We can use the fcn .withColumn() to add a new column
 * Lets make one and call it pass in first param. Second param is a col obj but we need to dynamically build the column. 
 * We can build a column on the fly with the .lit() method. It means literal, we are passing a literal value
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); 
		
//		dataset = dataset.withColumn("pass", lit("YES")); //first iteration, every value in the new col is YES
/* subject | pass
 * Math | YES
 * ...
 * 
 * We can put dynamic code in this though. 
 */
		
		dataset = dataset.withColumn("pass", lit( col("grade").equalTo("A+")  )); //this will return TRUE if the grade col is A+. Otherwise value of FALSE will be returned
		
/* grade | pass
 * A+ | true
 * C | false
 * ...
 * 
 * That was pretty easy! Getting complex logic into this kind of code would be hard for us to say grades A+, A, B, C are passing. THis is where UDFs come in. We can add our own fcn
 * into spark API and we can call them like our previous fcns. Cant do this with just standard java we have to follow some precise steps in Spark API to register the fcn.
 * We do this with the spark session object. Do this with .udf() this doesnt do anything on its own but we can call .register() afterwards. This is a heavily overloaded method so so
 * dont follow the intellisense. 
 * First param - name of fcn. 
 * Second param - java implementation of fcn. Modern way is to use a lambda fcn. Will do that first then see how to do it with old fashion way.
 * Third param - return type of expression in spark terms. For us its true or false so in spark terms thats DataTypes.BooleanType
 */
		spark.udf().register("hasPassed", (String grade) ->  grade.equals("A+"),  DataTypes.BooleanType); //modern java code implementation with lambda. Dont use .equalTo use .equals

/* The fcn `hasPassed` should now be registered in Spark. We can use this in SQL syntax, java api or whichever. Can use with calling .mean(), .avg() fcns. 
 * lets test if it works. Call the UDF with the fcn callUDF() first param is name of fcn we call and second is which cols are we passing as input
 */
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade") )); //and it works
		

		dataset.show(100); 
		
		spark.close();
	
	}
}
