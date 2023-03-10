package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson102 {

/* Lesson 102 Correlation Features
 * 
 * Idea of correlation is, we ask Spark what is correlation between sqft living (living space) and value we are trying to predict. Spakr gives us a number between -1 and 1.
 * 0 - no correlation / no relationship
 * 1 - strong correlation (positive)
 * -1 - strong correlation (negative)
 * 
 * 1 means size of property increase, price increase. -1 means size of property increases, price DECREASES. A feature with a strong correlation is a good parameter for our model.
 * Feature with no correlation / close to 0 not good in our model. Lets find out what correlations are for each input variable. Do this with .stat() method
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
				.csv("src/main/resources/kc_house_data.csv"); 

//		csvData.describe().show();
		
		System.out.println("Correlation between price and sqft_living is " +
				csvData.stat().corr("price", "sqft_living") //.cor() is correlation, two parameters of the two fields we want to find correlation between. returns double so we can print this out
		);
/* Correlation between price and sqft_living is 0.702035
 * 
 * Okay tso this value is close to 1 which means it is a strong correlation and as sqft_living increases, price increases. positive correlation.
 * We want to find correlation for many variables so lets do this a different way by putting variables in a loop. First drop from csv dataset fields we are uninterested in.
 */
		csvData = csvData.drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode", "lat", "long"); //everything to exclude
		
		for (String col : csvData.columns()) { //for each loop to go through each col name
			System.out.println("Correlation between price and " + col + " is " + csvData.stat().corr("price", col) );
		}
/* price 1.0
 * bedrooms 0.308
 * bathrooms 0.525
 * sqft_living 0.702
 * sqft_lot 0.089
 * floors 0.256
 * sqft_above 0.6055
 * sqft_basement 0.3238
 * yr_built 0.0540
 * sqft_living15 0.5853
 * sqft_lot15 0.0824
 * 
 * We are interested to see which variables have potential correlation between variable name and price and which ones have little correlation.
 * Going down the list, price and itself is exact match so useless to us.
 * Bedrooms has positive correlation and bathrooms. 
 * Sqft_living has high correlation.
 * sqft_lot is close to zero, exclude.
 * year built low correlation too, exclude that.
 * 
 * Exclude the ones with low correlation and keep the high correlations
 */
		
		
	}
}