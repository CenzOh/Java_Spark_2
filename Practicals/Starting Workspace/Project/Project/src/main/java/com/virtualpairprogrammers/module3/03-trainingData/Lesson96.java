package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson96 {
	
/* Lesson 96 Practical Walkthrough
 * Lets do it together. First create a set of features which need to be a vector.
 */
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv"); //changed
		
//		csvData.printSchema();
//		csvData.show();
		
//		VectorAssembler vectorAssembler = new VectorAssembler(); 
//		vectorAssembler.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"} ); 
//		vectorAssembler.setOutputCol("features");
		
// we can actually shortcut this a bit. Each of these methods set the values for vector assembler but ALSO return a vector assembler. SO we can chain the methods together.
		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"} )
				.setOutputCol("features");
		
//now lets take our CSV data and transform it with our vector assembler. We will set the results to be a data set of rows and we'll call this one model input data.
//gives us csvData with extra col called features. Lets drop all cols except price and features
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
		modelInputData.show();
		
/*
 * label | features
 * 221900 | [3.0,1.0,1180.0]
 */

	}
}