package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson98 {

/* Lesson 98 Assessing Model Accuracy with R2 and RMSE
 * 
 * There are ways to assess whether a model is good or not other than looking at the predictions and comparing to labels. Actual statistical measures. Which statistical measures
 * that are available depends on the model. FOr linear regression, we have two: root mean squared error RMSE and the r squared value. We wont go into detail about what they are. 
 * Not difficult to calculate tho. One thing to know, for RMSE know that the smaller the number the better. FOr R2 its a measure between 0 and 1, so closer to 1 is better.
 * 
 * If we had two models and one model had a LOWER RMSE and a HIGHER R squared. That model would be BETTER than the first model. If RMSE is very small and R squared is close to 1, generally
 * will be a good model. Thats what we want. Spark will calculate these values for us in training AND testing data.
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
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living"} )
				.setOutputCol("features");
		
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
//		modelInputData.show();
		
		Dataset<Row>[] trainingAndTestData = modelInputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = trainingAndTestData[0]; 
		Dataset<Row> testData = trainingAndTestData[1]; 
		
		LinearRegressionModel model = new LinearRegression().fit(trainingData);
		
//before transformation and after fitting the model, we can find out the r square and RMSE values for the model.
		System.out.println("Training data r2 value is " + model.summary().r2() + " and the RMSE is " + model.summary().rootMeanSquaredError()); 

//.summary() returns a LinearRegressionTrainingSummary. Methods .r2() and .rootMeanSquaredError() will give us what we are looking for. 
		
		model.transform(testData).show(); 
		
// lets see the r2 and RMSE values are for the testing dataset
		System.out.println("Testing data r2 value is " + model.evaluate(testData).r2() + " and the RMSE is " + model.evaluate(testData).rootMeanSquaredError()); 
		
//this time call model.evaluate() passing in the model we want to evaluate then call .r2 and RMSE. We dont even have to run the transformation to do the evaluation
		
/* Training data r2 value is 0.50146 and the RMSE is 256742.03880
 * Testing data r2 value is 0.52539 and the RMSE is 262194.4387
 * 
 * Okay well what does this tel us? r2 is between 0-1. Our value is close to a hafl. So not good. RMSE looks big so we know this is not a good model. What we do know is that because
 * the two values are relatively close to each other for both testing and training, the splits are sensible. What could have happened (unlikely with randomsplit) is suppose all the low value properties
 * were in testing and there was NO lower values in the training. That would mean the testing splits are not good and be bad representation for data. Can happen with outliers.
 * We need to find ways to improve the measures of accuracy and make it better. Some things we can do we will learn future chapters.
 * Currently we can add extra features in the model. Lets add three more specifically lets add sqft_lot, floors, and grade. Let me try it on my own first.
 */
		
		// MY VERSION - instrucotr version exactly the same
		VectorAssembler vectorAssemblerExample = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"} )
				.setOutputCol("features");
		
//everything else should be the same. Lets find out if it improves our results
/* Training data r2 value is 0.5439 and the RMSE is 248356.3744
 * Testing data r2 value is 0.55904 and the RMSE is 242069.907
 * 
 * Well we can see our r2 is higher closer to 0.56. And the RMSE is a bit lower. Some more things we can do to improve. Will cover more later. Lets talk about model fitting parameters next.
 */ 
		
	}
}
