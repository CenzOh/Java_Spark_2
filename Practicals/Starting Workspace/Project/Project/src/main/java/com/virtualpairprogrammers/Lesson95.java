package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson95 {

/* Lesson 95 working with data from kaggle.
 * 
 * We must be careful when working with different data sets on kaggle because we may have to do some manipulation to be able to use them. Maybe theres missing values, forced data in outliers,
 * etc. We have to do some analysis to clean up the data. We wont do that in this module since we covered it earlier. THis data chosen doesnt really need much cleanup.
 * Our data file is called kc_house_data.csv. New class to use this called HousePriceAnalysis.java. We can use some of the starter code from the earlier classs
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
		
		csvData.printSchema();
		
		csvData.show();
		
/* This is a much bigger file with lots of columns. THe idea is the price column is our label, the value we want to predict. We have all sorts of prices. Other cols are num of bedrooms,
 * num of bathrooms. We can see the numbers are calculated in a consistent way that is what is important to see. Living space in sq feet, lot in sq ft, etc. Any of these attributes
 * can be a parameter for our linear regression. At this stage, some of the parameters WILL be good, others wont. We will not include every parameters. We will learn how to select
 * parameters later on. For now lets use 2 or 3 to build a model using them. Lets use num of bedrooms, bathrooms, and sq foot living size. 
 * 
 * label = price
 * features = vector of bedrooms, bathrooms, sqft_living
 * 
 * Now I should try this on my own to see if I can build the model
 */
		//my version - just create the data set DONT build the model yet.
		VectorAssembler vectorAssembler = new VectorAssembler(); //create the vector assembler instance
		vectorAssembler.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"} ); //set col names as input
		vectorAssembler.setOutputCol("features"); //name our new output col to be features
		Dataset<Row> csvDataFeatures = vectorAssembler.transform(csvData); //transform our data into this form
		Dataset<Row> inputData = csvDataFeatures.select("price", "features").withColumnRenamed("price",  "label"); //select only label and features, rename price col to label
		
/*
 * label | features
 * 221900 | [3.0,1.0,1180.0]
 * ...
 */
	
	}
}
