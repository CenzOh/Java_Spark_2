package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson99 {

/* Lesson 99 setting linear regression parameters
 * 
 * We will be talking about model fitting parameters. In last few chapters we saw the following warning:
 * 
 * WARN WeightedLeastSquares: regParam is zero, which might cause numerical instability and overfitting.
 * 
 * We must deal with this now. THis error message says we havent supplied a value for regParam. The process Spark uses how to come up with coefficients of the model to build is
 * by using a number of different regression analysis methods. The linearRegression object, the helper obj to create the model when we call .fit(). Its really a black box.
 * It analyzes the data we filled into the model builder, does some complex mathematics to create the linear regression model at the end. There are a number of these mathematical
 * algorithms that take place within it like elastic net, bridge, laso, linear squares. These algorithms can take optional parameters to affect their output.
 * 
 * A little vague with the following since to go into more detail we need to have a better undersatnding of those advanced statistical techniques which is out of scope fo the course. 
 * When we set up the model we can provide these parameters optionally. By providing different values for the parameters it can result in a better model. Lets experiment with the different
 * numbers to feed in. Spark makes it easy for us to do this. What are the parameters first of all? We can find our answer in spark API documentaion under linearRegression.
 * 
 * org.apache.spark.ml.regression.LinearRegression class, scroll down to set methods. Can see there are a lot. Some we may be used to looking at are setAggregationDepth(), setElasticNetParam(),
 * setEpsilon(), setRegParam(), setTol(). We can experiment and set values for these parameters. The documentation is a bit sparse tho, click on one of the methods dont really see
 * much help. 
 * 
 * However, if we look at the spark general documentaion, there are some examples for linear Regression. They show a setting for the following:
 *  .setMaxIter(10)
 *  .setRegParam(0.3)
 *  .setElasticNetParam(0.8)
 *  
 * Good starting point for which parameters to set. FOr the values, thats dependent on the shape of the data we have. But we wont have good values to pick unless we have an understanding
 * how to interpret the shape of tehd ata. SO lets try the example.
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

		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"} ) //we added more features from last time.
				.setOutputCol("features");
		
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
//		modelInputData.show();
		
		Dataset<Row>[] trainingAndTestData = modelInputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = trainingAndTestData[0]; 
		Dataset<Row> testData = trainingAndTestData[1]; 

/*  OLD VERSION
 * Training data r2 value is 0.5439 and the RMSE is 248356.3744
 * Testing data r2 value is 0.55904 and the RMSE is 242069.907
 * 
 * NEW VERSION with set functions
 * Training data r2 value is 0.54403 and the RMSE is 243942.131753
 * Testing data r2 value is 0.55458 and the RMSE is 259711.23207
 * 
 * We can see we set max iterations to 10 and the warning about the regParam dissappeared. Extra noise printed out for each parameters.
 * Looks like r2 increased slightly. Improved model slightly. Would be sensible if we can try lots of different values and see if we can come up with one that gives us a better model.
 * Good news is that spark gives us a way to automate the testing. We can say in spark, lets come up with a set of values for each of the different parameters for elastic net and reg for instance.
 * 					Elastic net X axis 
 *      				 | 0 | 0.5 | 1
 * Reg Y axis		0.01 | X | X   | X
 * 					0.1  | X | X   | X
 * 					0.5  | X | X   | X 
 * 
 * Making a grid! 9 possible combinations. Reruns process with the nine combos for the best answer. We have to define what best means. Maybe say maximize our squared and minimize our
 * RMSE, etc. Give spark this grid of values with the way we want the best responses. It will then give us the best model. Do this by creating an array of param map objects.
 * Each array entries contain one combo of different param values and spark builds those into a grid to give us an objekct to create this array.
 */
		
//remove this and start again. COde will look different than what we wrote here
//		LinearRegressionModel model = new LinearRegression()
//				.setMaxIter(10)
//				.setRegParam(0.3)
//				.setElasticNetParam(0.8)
//				.fit(trainingData);
		
		LinearRegression linearRegression = new LinearRegression();
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder(); //can use helper object param grid builder
//create array of param maps call .addGrid() each time for each variable. First parameter is obj type double param get that from linear regression object
//then provide an array of possible values to use for this parameter

		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
				.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
				.build(); //this creates array of param map.
		
//		System.out.println("Training data r2 value is " + model.summary().r2() + " and the RMSE is " + model.summary().rootMeanSquaredError()); 
		
//		model.transform(testData).show(); 
		
//		System.out.println("Testing data r2 value is " + model.evaluate(testData).r2() + " and the RMSE is " + model.evaluate(testData).rootMeanSquaredError()); 
			

			

	}
}
