package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson93 {

/* Lesson 93 Model Fitting
 * 
 * We are ready to build the model. Lets go through an overview of the process. Each model type in spark has a helper object to create the model. For linear regression this obj is
 * called LinearRegression. We will create a new linear regression and set some properties / attributes. We will not do that right now, will do it later. THen we have a method
 * called .fit() which takes a dataset (in correct format) with a col of labels and col of features, .fit() will result in new obj which is the actual fitted model.
 * It will be type LinearRegressionModel. LRM has some properties we can inspect. We want to look at intercept and coefficient to find the formula that was created for us.
 * There is a mehtod of the model called transform which lets us pass in the dataset of label and features and outputs same data with new col, predictions. Predictions col is what we
 * get when we apply the formula we made in the model.
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
				.csv("src/main/resources/GymCompetition.csv");
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight"} );		
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
		
//		csvDataWithFeatures.show();
		
		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
		LinearRegression linearRegression = new LinearRegression(); //new instance of linear regression, wont set properties right now
		LinearRegressionModel model = linearRegression.fit(modelInputData); //make sure to import from MLib. Use .fit() to fit our model
		System.out.println("The model has intercept " + model.intercept() + " and coefficients " + model.coefficients()); //lets take a look at our intercept first
		
/* The mode has intercept 33.488 and coefficients [0.0480, -0.0786637, 0.40272]
 * This is saying the formula we came up with (lets look in reverse order) take the weight and multiply it by 0.4073 then we take height and multiply by -0.0786 and then take age and
 * multiply that by 0.0480 and then we add on 33.488. More complex than our simple example. Hopefully this is more accurate formula. Also quick note there are a number
 * of warnings we can ignore them at the moment and will look at the first later on. The rest are annoying information we can ignore them.
 * 
 * WARN WeightedLeastSquares: reParam is zero, which might cause numerical instability and overfitting.
 * ...
 * 
 * Is our formula good? We will look at how to figure that our later. FOr now lets call the transform model and look to see if the predictions look close to the label or not.
 */
		model.transform(modelInputData).show(); //pass in the dataset. What we are doing is NOT PRODUCTION STANDARD. NOT GOOD. But lets look at it for now. Lets show on screen not save results.
		
/* label | features | prediction
 * 55 | [23.0,180.0,88.0] | 55.874
 * ...
 * 
 * Results look pretty good. Our label is 55, prediction is 55.87. Actual is 54, pred is 54.88. There are some a bit further off but overall very good.
 */
		
	}
}