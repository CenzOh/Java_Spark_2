//final version of this code
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.*; //now we dont have to explicitly refer to .functions()
import org.apache.spark.sql.types.DataTypes;


public class ExamResults {
		
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/exams/students.csv"); 

		
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
		


		dataset.show(100); 
		
		spark.close();
	
	}
}


