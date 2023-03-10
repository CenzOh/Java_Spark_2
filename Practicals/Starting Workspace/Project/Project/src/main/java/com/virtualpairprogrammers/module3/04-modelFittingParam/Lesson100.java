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

public class Lesson100 {

/* Lesson 100 Training Testing and Holdout data
 * 
 * Recall when we will split our data, 80% will be training and 20% will be for testing / evaluating. Now we will ask Spark to run the model building process tons of times to see which is
 * the best set of parameters. So we will split our data into three sections. Largest part to build the model, Spark uses next part to evaluate which is the best one, finally we will
 * use the third set of data to do our own evaluation to see if Spark produced a good model or not. Correct terminology is Training data, Testing data, Hold out data. 
 * 
 * SPark uses the Test data when it creates lots of models to find the best fitting model. We will use holdout data to decide if the model is good or not.
 * Recall when we originally did 80-20 split we created one data set for training and testing, and a second data set for hold out. Now we will split that first dataset into two
 * separate data set. Spark will do that for us. We have to create a train validation split. This object requires us to set four values:
 * First is the estimator, this is the model builder we are using (linear regression).
 * Second is evaluator, the instruction to say how we will determine best model from the result (such as maximize r squared, minimize the root mean squared error).
 * paramMap, which we just created.
 * Finally, trainRatio which is what proportion of the data are we supplying should be used for training? We can do 0.8, 80 percent training, 20 percent testing.
 * 
 * Why do we need the hold out data? Why cant spark do the job with just testing and training? Its because test data is being used every single time, it will become of the model building process.
 * Spark will give us the best model that fits the test data. Might not be the best model we can get so we need to do an independent verification that what Spark produces for us is
 * a good model. SO we reserve / hold back some data to use to do that evaluation. THis data is not used in model building process at all, completely independent for a fair view that
 * what Spark produced is good. 
 * 
 * If the model fits test data but not hold out data this is known as OVERFITTING - We improved the model so much that it works for test data but not for general datasets. AVOID THIS with holdout
 * data. 
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
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"} )
				.setOutputCol("features");
		
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
//		modelInputData.show();
		
		Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[] {0.8, 0.2}); //renamed from trainingAndTestData to dataSplits
		Dataset<Row> trainingAndTestData = dataSplits[0]; //renamed from traingData to trainingAndTestData
		Dataset<Row> holdOutData = dataSplits[1];  //renamed from testData to holdOutData
		
/* Recap above ^^ We split our data into two sections using the 80-20 ratio. First section is being used for training and testing. Second set is the hold out for our independent evaluation.
 * Let us build the train validation split object.
 */
		
		LinearRegression linearRegression = new LinearRegression();
		
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
				.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
				.build();
		
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit() //import trainValidationSplit and RegressionEvaluator()
				.setEstimator(linearRegression) //set the model we want to use, which in our case is linear regression
				.setEvaluator(new RegressionEvaluator().setMetricName("r2")) //"rmse" for root mean squared in .setMetricName(), takes in string argument
				.setEstimatorParamMaps(paramMap) //set paramMap object which we created
				.setTrainRatio(0.8); //training is 80% rest is used for testing

/* The trainValidationSplit object will produce the model for us. It can take any type of model builder, it is currently wrapping the linear regression.
 * When we run the fit method, the method to build our model, we will not get a linear regression model out of it. We'll get a trainValidationSplit model and we need to extract the linear reg model
 */
		TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData); //import TrainValidationSplitModel. Extract with .bestModel() method
		LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel(); //returns linear reg model. This is a wrapper obj so itll contain a generic model type. Cast to right data type

//import LinearRegressionModel from ml
		
		
		
		System.out.println("Training data r2 value is " + lrModel.summary().r2() + " and the RMSE is " + lrModel.summary().rootMeanSquaredError()); //call lrModel instead of model now 
		
//		model.transform(testData).show(); 
		
		System.out.println("Testing data r2 value is " + lrModel.evaluate(holdOutData).r2() + " and the RMSE is " + lrModel.evaluate(holdOutData).rootMeanSquaredError()); //change testData to holdOut
			
/* This will take longer to run since the model is being built several times.
 * 
 * Training data r2 value is 0.55016 and the RMSE is 249552.6157
 * Testing data r2 value is 0.53081 and the RMSE is 237323.19560
 * 
 * This is a minor improvement. Now we can add in more parameters and see if that helps us. Process works well though. There are other things we can do to imrpove the model (more on that later).
 * Lets get more information about our model like the coefficients and intercept
 */
		
		System.out.println("coefficients " + lrModel.coefficients() + " intercept: " + lrModel.intercept());
		System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam()); //use getRegParam if we want to query these values
		
/* coefficients: [-43818.75448...
 * reg param: 0.01 elastic net param: 0.5
 * 
 * The reg param is in fact the SMALLEST of the values we supplied so we may get a better model with even smaller values. Can experiment with that if we want.
 * But lets look at using other attributes of a house price to see if the other attributes can help improve the model.
 */

	}
}