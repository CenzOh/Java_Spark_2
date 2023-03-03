// Lesson 112 and 113
package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VPPChapterViewsLogistic {

	
	public static void main(String[] args) { 
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("VPP Chapter Views")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true)
				.csv("src/main/resources/vppChapterViews/*.csv");   //reading csv the same.
		
//		csvData.show(); //after reading in looks good. Lets keep checking as we go.

//filter to ONLY get records that still have a subscription, remove the two cols we dont want same as usual.
		csvData = csvData.filter("is_cancelled = false").drop("observation_date","is_cancelled"); 
		
 // 1 - Customers watched NO videos (who we want), 0 - Customers watched some videos

		csvData = csvData.withColumn("firstSub", when( col("firstSub").isNull(), 0 ).otherwise(col("firstSub")) ) 
				.withColumn("all_time_views", when (col("all_time_views").isNull(), 0).otherwise(col("all_time_views"))) 
				.withColumn("last_month_views", when (col("last_month_views").isNull(), 0).otherwise(col("last_month_views")))
				.withColumn("next_month_views", when (col("next_month_views").$greater(0), 0).otherwise(1)); 
//changed above line to check if next month views col is greater than 0. $greater is valid syntax and is the name of the function. If its greater than 0, return 0 for label. OTHERWISe return 1

		csvData = csvData.withColumnRenamed("next_month_views", "label");

//indexing and encoder is still the same
		
		StringIndexer payMethodIndexer = new StringIndexer(); 
		csvData = payMethodIndexer.setInputCol("payment_method_type")
			.setOutputCol("payIndex")
			.fit(csvData)
			.transform(csvData);
		
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country")
				.setOutputCol("countryIndex")
				.fit(csvData)
				.transform(csvData);
		
		StringIndexer periodIndexer = new StringIndexer();
		csvData = periodIndexer.setInputCol("rebill_period_in_months")
				.setOutputCol("periodIndex")
				.fit(csvData)
				.transform(csvData);
		
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"payIndex", "countryIndex", "periodIndex"})
			.setOutputCols(new String[] {"payVector", "countryVector", "periodVector"})
			.fit(csvData).transform(csvData);
		
// vector assembler the same
 
		VectorAssembler vectorAssembler = new VectorAssembler();  
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"firstSub","age","all_time_views","last_month_views",
				"payVector","countrVector","periodVector"}) 
			.setOutputCol("features")
			.transform(csvData).select("label", "features"); 
		

		
		Dataset<Row>[] trainAndHoldoutData = inputData.randomSplit(new double[] {0.9, 0.1}); //90% 10% split.
		Dataset<Row> trainAndTestData = trainAndHoldoutData[0];
		Dataset<Row> holdOutData = trainAndHoldoutData[1];

		LogisticRegression logisticRegression = new LogisticRegression(); //no linear reg anymore
		
		ParamGridBuilder pgb = new ParamGridBuilder();
		ParamMap[] paramMap = pgb.addGrid(logisticRegression.regParam(), new double[] {0.01, 0.1, 0.3, 0.5, 0.7, 1}) //reg param and elastic net param can be refered to by log reg
		   .addGrid(logisticRegression.elasticNetParam(), new double[] {0,0.5,1})
		   .build();
		
		
		TrainValidationSplit tvs = new TrainValidationSplit();
		tvs.setEstimator(logisticRegression) //change to log reg
		   .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
		   .setEstimatorParamMaps(paramMap)
		   .setTrainRatio(0.9);
		
		TrainValidationSplitModel model = tvs.fit(trainAndTestData);
		
		LogisticRegressionModel lrModel = (LogisticRegressionModel)model.bestModel(); //pick the ML library for the import
		
		System.out.println("Accuracy value is " + lrModel.summary().accuracy()); //we dont have an r2 value, use accuracy score
		System.out.println("coefficients " + lrModel.coefficients() + " intercept: " + lrModel.intercept()); 
		System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam());
		
/* Accuracy value is 0.79611
 * coefficients : [-0.07385
 * reg param : 0.01 elastic net param 0.0
 */
		
		LogisticRegressionSummary summary = lrModel.evaluate(holdOutData); //lets just save this evaluation in a variable to make it cleaner

		double truePositives = summary.truePositiveRateByLabel()[1];
		double falsePositives = summary.falsePositiveRateByLabel()[0];
		
		System.out.println("For holdout data, likelihood of positive being correct is " + (truePositives / (truePositives + falsePositives)));
		System.out.println("Holdout accuracy is " + summary.accuracy()); //find out accuracy of holdout data too

/* Positive being correct 0.930232
 * Holdout accuracy is 0.768488
 */
		lrModel.transform(holdOutData).groupBy("label", "prediction").count().show();
		
/* label | prediction | count
 * 1	 | 0.0		  | 11		falseNegative
 * 0	 | 0.0  	  | 53		trueNegative
 * 1	 | 1.0		  | 203		truePositive
 * 0 	 | 1.0 		  | 57		falsePositive
 */	
	}
}