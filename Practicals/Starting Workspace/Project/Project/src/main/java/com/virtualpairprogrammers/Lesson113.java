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

public class Lesson113 {

/* Lesson 113 Coding a log reg
 * 
 * Lets write some code to see how log reg model works. We can reuse the case study from last chapter. Recall we used that example to predict how many chapters a customer may watch next month.
 * Result was not very accurate. We simply want to know if customers watch videos next month or not. We are interested in the customers who will not watch videos. So log reg
 * can help us with this example. Lets take that code and make it log reg. Make new class called `VPPChapterViewsLogistic`
 */
	
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
		
/* We want to change our label to be a 1 or 0. Whether or not they will watch videos next month. Label is based on next months views.
 * Then for our next month views, dont check if its NULL and replace, instead check if its greater than 0. If it is greater than 0 (customer watched some videos) turn label into a 0.
 * 
 * 1 - Customers watched NO videos (who we want), 0 - Customers watched some videos
 */
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
 * 
 * Accuracy about 80%, not too bad, better result than what we had with linear regression. So what is the real measure we are looking for? Well lets think about it:
 * If we predict a positive, that means we think this customer will NOT watch any videos next month. So we want to persuade them to watch some videos. 
 * If we predict a positive, what is the likelihood that it really is a positive? (true positive). This will be our first version. Find the value of true positives / total value all positives.
 * Do this on the hold out data. We can call lrModel.evaluate() and put holdOutData in there. Next we have two methods, false positive rate by label and true positive rate by label.
 * 
 * Instructor thinks these are poorly named and are confusing. Little documentation on them as well. Heres how to understand: Key is to ignore POSITIVE in method name
 * read as true rate by label or false rate by label.
 * 
 * If we consider true positives, this is when ( label = 1, prediction = 1 ) we want the truePositiveRateByLabel()[1] with the 1 as input.
 * How about 	  true negatives, 			   ( label = 0, prediction = 0 ). 			 truePositiveRateByLabel()[0] 
 * 		Label is 0. THis tells us true negative even tho method name has truePositive in it.
 * 
 * Similarly for  false positives, 			   ( label = 0, prediction = 1). 			 falsePositiveRateByLabel()[0]. 
 * 				  false negatives, 			   ( label = 1, prediction = 0).			 falsePositiveRateByLabel()[1]
 * 
 * Again ignore the word Positive in the method names for this to make sense. In our example we want to find TRUE POSITIVES and FALSE POSITIVES. So we want truePositiveRateByLabel()[1] 
 * and falsePositiveRateByLabel()[0]. Store true positives as a double.
 */
//		double truePositives = lrModel.evaluate(holdOutData).truePositiveRateByLabel()[1]; //[1] Position 1 of the array
//		double falsePositives = lrModel.evaluate(holdOutData).falsePositiveRateByLabel()[0];
		
		LogisticRegressionSummary summary = lrModel.evaluate(holdOutData); //lets just save this evaluation in a variable to make it cleaner

		double truePositives = summary.truePositiveRateByLabel()[1];
		double falsePositives = summary.falsePositiveRateByLabel()[0];
		
		System.out.println("For holdout data, likelihood of positive being correct is " + (truePositives / (truePositives + falsePositives)));
		System.out.println("Holdout accuracy is " + summary.accuracy()); //find out accuracy of holdout data too
/* Positive being correct 0.930232
 * Holdout accuracy is 0.768488
 * 
 * Okay thats pretty good. Recall our initial lrModel accuracy is about 80%. Overall accuracy score WITH the data is about 77%. But overall likelihood of a positive being correct is 93%. 
 * If we predict customers likely to watch no videos, 93% accuracy that they wont watch a video. Good here for us to remind the customers we have some videos they can watch!
 * 
 * Last thing to discuss is the size of true and false positives and underlying numbers to calculate 93%. Easy to do, just transform the lrModel and we can summarize the results with standard
 * manipulation in Spark, not ML.
 * So simply put, take the log reg model, transform to our hold out data. group by the label and pred column. use .count() to see how many in each of these groupings. .show() print to screen
 */
		lrModel.transform(holdOutData).groupBy("label", "prediction").count().show();
		
/* label | prediction | count
 * 1	 | 0.0		  | 11		falseNegative
 * 0	 | 0.0  	  | 53		trueNegative
 * 1	 | 1.0		  | 203		truePositive
 * 0 	 | 1.0 		  | 57		falsePositive
 * 
 * Lets start with prediction of 1, customer wont watch video. Label of 1 is our truePositive. We correctly predicted customer d/n watch video 203 times.
 * The label = 1, pred = 0 is when they didn't watch videos and we didn't manage to predict it 11 times. Thats good. We're getting almost all the customers who wont watch videos
 * label = 0, pred = 1, this is the falsePositive in which these customers DID watch some videos but we predicted they wont watch 57 times. Not too worried about this in our example.
 */
		
	}
}