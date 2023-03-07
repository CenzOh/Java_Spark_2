package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
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

public class Lesson106_3 {

/* Lesson 106 Understanding Vectors - PART 3 With instructor to work on the house price analysis with indexer and encoder
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

		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))); 
		csvData.show();
		
/* ******** DO our indexer BEFORE the vector assembler. Lets do condition first
 */
		StringIndexer conditionIndexer = new StringIndexer();
		conditionIndexer.setInputCol("condition");
		conditionIndexer.setOutputCol("conditionIndex");
		conditionIndexer.fit(csvData).transform(csvData);

//grade indexer
		
		StringIndexer gradeIndexer = new StringIndexer();
		gradeIndexer.setInputCol("grade");
		gradeIndexer.setOutputCol("gradeIndex");
		gradeIndexer.fit(csvData).transform(csvData);
		
//zipcdode indexer
		
		StringIndexer zipcodeIndexer = new StringIndexer();
		zipcodeIndexer.setInputCol("zipcode");
		zipcodeIndexer.setOutputCol("zipcodeIndex");
		zipcodeIndexer.fit(csvData).transform(csvData);
		
//check if its okay asnd comment out rest of code to see if all is good
//		csvData.show();
		
/* ... sqft_above_percentage | conditionIndex | gradeIndex | zipcodeIndex
 * 	   1.0                   | 0.0            | 0.0        | 45.0 
 * ...
 * 
 * Looks pretty good and they ahve been indexed correctly. zip code goes up a reasonable amount like 59. Think about that the zipcode couldve been a 60 plus element array super inefficient
 */
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"conditionIndex", "gradeIndex", "zipcodeIndex"}); //remember can do this on multi columns
		encoder.setOutputCols(new String[] {"conditionVector", "gradeVector", "zipcodeVector"});
		csvData = encoder.fit(csvData).transform(csvData);
		
//check results
		csvData.show();
		
/* conditionVector | gradeVector   | zipcodeVector
 * (4,[0],[1,0]    | (11,[0],[1,0] | (69,[45],[1,0]
 * 
 * We can see zipcode had 70 different categories wow! Lets add these to our model now in the vectorAssembler
 */

		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade", "conditionVector", "gradeVector", "zipcodeVector", "waterfront"} )
				.setOutputCol("features"); //adding our vector fields to input cols also including waterfront didnt need to make waterfront a vector since its already a binary value
	
/* train r2 0.825 RMSE 154783.37
 * test r2 0.8217 RMSE 148650.19784
 * 
 * Wow dramatically better results. Our r square are about 0.82 and RMSE significantly lower. Not perfect but much better model. We added in very good fields that made a difference
 * to our model.
 */
		
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
//		modelInputData.show();
		
		Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingAndTestData = dataSplits[0];
		Dataset<Row> holdOutData = dataSplits[1]; 
		
		
		LinearRegression linearRegression = new LinearRegression();
		
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
				.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
				.build();
		
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit() 
				.setEstimator(linearRegression) 
				.setEvaluator(new RegressionEvaluator().setMetricName("r2")) 
				.setEstimatorParamMaps(paramMap) 
				.setTrainRatio(0.8); 

		TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData); 
		LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel(); 		
		
		
		System.out.println("Training data r2 value is " + lrModel.summary().r2() + " and the RMSE is " + lrModel.summary().rootMeanSquaredError()); //call lrModel instead of model now 
		
//		model.transform(testData).show(); 
		
		System.out.println("Testing data r2 value is " + lrModel.evaluate(holdOutData).r2() + " and the RMSE is " + lrModel.evaluate(holdOutData).rootMeanSquaredError()); 
		
		System.out.println("coefficients " + lrModel.coefficients() + " intercept: " + lrModel.intercept());
		System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam());
	}
}