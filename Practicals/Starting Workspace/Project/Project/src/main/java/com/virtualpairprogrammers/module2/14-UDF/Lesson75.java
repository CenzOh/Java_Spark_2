package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class Lesson75 {
	
/* Lesson 74 Using more than one parameter in UDF
 * Ok lets now add more than just A+. Lets say if grade is A,B, or C is pass as well. Lets exapdn this out as well and make it more complex
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); 
		
//BEFORE
//		dataset = dataset.withColumn("pass", lit( col("grade").equalTo("A+")  ));
		
//OLD UDF w/ LAMBDA
//		spark.udf().register("hasPassed", (String grade) ->  grade.equals("A+"),  DataTypes.BooleanType);


		
//writing our new version of the UDF with lambda and multi parameters
		spark.udf().register("hasPassed", (String grade) -> {
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"); //check if grade contains these letters. This accounts for both A and A+. Works!
			
		},  DataTypes.BooleanType);
		
//CALLING OLD UDF
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade") ));

/* grade | pass
 * D | false
 * C | true
 * A+ | true
 * ...	
 * 
 * This looks good. We can expand this out to be as complicated as we like which is good. Dont have to use just one param. lets keep expanding it with funny rules. Lets sau
 * in biology exam we will make it so you need A or A+ to pass biology. Keep the first set of rules too. Two params now. 
 */
		spark.udf().register("hasPassed", (String grade, String subject) -> {
			
			if (subject.contentEquals("Biology")) { //if subject is biology strict grading structure
				
				if (grade.startsWith("A")) return true; //if biology grade is A or A+ return true
				return false; //otherwise return false
			
			} //bottom will execute if course is NOT biology 
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"); 
			
		},  DataTypes.BooleanType);
		
//upgrade call to the UDF
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject") )); //ordering is important! 
		
/* subject | Grade | pass
 * biology | C | false
 * Philosophy | B | true
 * ...
 */


		dataset.show(100); 
		
		spark.close();
	
	}
}
