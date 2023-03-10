package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson106_2 {


/* Lesson 106 Understanding Vectors - PART 2 ON MY OWN working on the house price analysis with indexer and encoder
 */
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();

/* ********** EXERCISE FOR ME - lets do this vector and add the extra features to the house price model for the variables Condition, Grade, Zipcode, and Waterfront.
 * Note - I have a 2 on variable names because I was originally including this in the 1 file but felt it was too big.
 * Waterfront is boolean so we can leave it as is. COndition, Grade, and Zipcode are categories
 */
			
			Dataset<Row> csvData2 = spark.read()
					.option("header",  true)
					.option("inferSchema", true)
					.csv("src/main/resources/kc_house_data.csv"); 

//			csvData.describe().show();
			
			csvData2 = csvData2.drop("id", "date", "view", "yr_renovated", "lat", "long"); //we want to add extra features condition, grade, zipcode, waterfront
			
//			for (String col : csvData.columns()) { 
//				System.out.println("Correlation between price and " + col + " is " + csvData.stat().corr("price", col) );
//			}
//			
//			csvData = csvData.drop("sqft_lot", "sqft_lot15", "yr_built", "sqft_living15"); //removing cols that have small correlation 
//			
//			for(String col1 : csvData.columns()) {
//				for (String col2 : csvData.columns()) {
//					System.out.println("Correlation between " + col1 + " and " + col2 + " is " + csvData.stat().corr(col1, col2) ); //removed price and added in col1 and col2
//				}
//			}
	//
//			csvData2 = csvData2.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))); 
//			csvData2.show();
			
			StringIndexer gradeIndexer = new StringIndexer(); //index grade, we know we have values here
			gradeIndexer.setInputCol("grade"); 
			gradeIndexer.setOutputCol("GradeIndex");
			csvData2 = gradeIndexer.fit(csvData2).transform(csvData2);
			
//			csvData.show();
			
			OneHotEncoderEstimator gradeEncoder = new OneHotEncoderEstimator();
			gradeEncoder.setInputCols(new String[] {"GenderIndex"});
			gradeEncoder.setOutputCols(new String[] {"GenderVector"});
			csvData2 = gradeEncoder.fit(csvData2).transform(csvData2);
			csvData2.show();
			
			VectorAssembler vectorAssembler2 = new VectorAssembler()
					.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage", "floors", "GradeVector" } ) //add our new GradeVector col name
					.setOutputCol("features");
			
			Dataset<Row> modelInputData2 = vectorAssembler2.transform(csvData2)
					.select("price", "features")
					.withColumnRenamed("price",  "label");
	
	}
}