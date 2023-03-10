package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson101 {

/* Lesson 101 Describing the Features
 * 
 * We will talk about how we select the parameters to build our model. Our current dataset has a ton of fields. We dont know which fields are good predictors of our outcomes.
 * The process to select suitable vairables for input into the model is a separate science so we will simply go over a high level overview of basic principles.
 * 
 * First issue, eliminate dependent variables. We dont want variables that DEPEND on the variable we are trying to predict (price). This should not be the case in our data set.
 * Imagine though that there was a field called sales tax. It contains amount of tax we would have to pay to buy the property. Sales tax is a percentage of that property.
 * This is a dependent variable. If we get a sales tax figure would we be able to accurately predict the property? No because if we dont know the price, we wont know the sales tax.
 * So the sales tax figure would be made up.
 * 
 * Second issue, understand a bit more about our data. DOes each variable have a sufficiently wide range of values? Helpful to know the minimum, maximum, and avg value of our labels and for
 * each features. We can find variation in the range too. If house range is between 100k - 1M but in our data most houses were around 700k then we know there isnt much variation.
 * Spark can give us this information with the .describe() method on a dataaset. 
 * 
 * We will make a new class called HousePriceFields.java again ill do mine in seperate lesson java files
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

//first lets call the describe method on our data set.
		csvData.describe().show(); //no input param will give info on every field in the dataset. describe() returns a new dataset so store it or call show() on results.
/* summary | id 
 * count | 21613
 * mean | 4.580301
 * stddev | 2.8765
 * ...
 * 
 * Ex of what we see, price field has a count of 21k rows. Avg price is 540k. Min is 75k and max is 770k. The standard deviation stddev we wont talk about in detial but know that if
 * house prices are distributed in normal distribution, that would mean there are more houses around avg price and few that are cheap and expensive. Our std dev tells us that about 68%
 * of all houses have a price between the mean +- std dev which is 540k += 367k. This tells us there is a wide range of house prices. Not normally distributed since about 70% of
 * all houses will appear between 173k and 900k but that goes above our max so that means there are more houses near top end of the range. So this is fine, good range.
 * 
 * Looking at ID will not help us too much since its a unique value for the house. Lets not include it. Date field is a string and our analysis wont work on string. We will look at how
 * to deal with next chapter. We can tell all the data is taken over a period of a year May 2nd 2014 - May 27th 2015. This is a good thing since house prices change over time. 
 * 10 year period would be too much so we would have to filer down or we have to adjust prices due to inflation.
 * 
 * Other interesting metrics, waterfront. It must be a 0 or a 1. Look at min and max. So this metric is really a boolean. Property is on waterfront or it isnt. Lets not include atm.
 * View field not sure what this means. Kaggle site will tell us view `has been viewed` probably how many times someone like a realtor or estate agent viewed the house. Not that useful for us.
 * How about grade? Overall grade given to house based on King County grading system. Sounds like this is categorical data. Property put into a category based on assessment. Can be a
 * good predictor of price. House with higher grade may be worth more. Lets consider this feature. Grade is a number between 1 - 13. We cant asume if higher number is better or worse.
 * Shouldnt use it like a number. More bedrooms, higher price but not the case here for grade. Lets exclude for now and treat it as non numeric. 
 * 
 * Next we have year built and year renovated. Year built will be helpful, older houses may be worth more or less. Relationship between age and price will def be there. Year renovated tho
 * not sure. Seems like the house is renovated or not since min is 0 and max is 2015. Maybe we should treat these as a boolean field. Next is zip code, this is cateogry field. Not a number
 * its a cateogry. Maybe one zip code is worth more than another. Property in zip code with higher number DOES NOT mean it is worth more, again this should be treated like category.
 * 
 * Fields we will use for now with type, we will look at some others we excluded in future chapter:
 * bedrooms integer
 * bathrooms double
 * sqft_living integer
 * sqft_lot integer
 * floors double
 * sqft_above integer
 * sqft_basement integer
 * yr_built integer
 * sqft_living15 integer
 * sqft_lot15 integer
 * 
 * So next lets find out if any variables have a clear relationship with the outcome we want to predict. We cant really create those types of graphs in spark but we can look at the relationship
 * between two variables which is called a correlation
 */
	
	}
}