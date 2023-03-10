package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson97 {

/* Lesson 97, splitting training data with random splits
 * 
 * This is a big data set, has about 21k rows. Its big enough to split down into two smaller data sets. One to train, one to test. How many records we choose to put in each dataset is up to us
 * but general rule is 80% trainings, 20% testing, to do this, datasets have a method called .randomSplit(). It takes in an array of doubles and we specify the percentage in each data set.
 * This method returns an array of datasets, one for training and one for testing.
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
//80% training, 20% testing. Our randomsplit will be different from instructors but results will be reasonably similar
		
		Dataset<Row> trainingData = trainingAndTestData[0]; //first element in the array
		Dataset<Row> testData = trainingAndTestData[1]; //second element in the array
		
//now let us set up and fit the model using the test data in a single line of code this time. new linearRegression() is our helper function
		
		LinearRegressionModel model = new LinearRegression().fit(trainingData);
		model.transform(testData).show(); //lets see what this looks like first
	
/* label | features | prediction
 * 90000 | [1.0,1.0,560.0] | 196390
 * ...
 * 
 * Our set of labels and features honestly dont have good predictions. Not a good model. No surprise since we only used 3 parameters. But also there are other things we can do.
 */
		
	}
}