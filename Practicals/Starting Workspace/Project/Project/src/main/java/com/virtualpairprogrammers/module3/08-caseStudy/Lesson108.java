package com.virtualpairprogrammers;

//for col() and when() fcns
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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


public class Lesson108 {

/* Lesson 108 Requirements for our Case Study
 * 
 * We will do a full practical example based on a real scenario the instructor used at Virutal Pair Programs. We will look at requirements and try on our own then there will be a walkthrough.
 * 
 * The issue is as follows: We have a num of subscribers, they pay a monthly fee and watch videos from our site. Some customer stops watching videos which means they may unsubscribe.
 * We want to identify who are the customers who might unsubscribe / who may stop watching the videos. If we can identify them, we can suggest to them videos they can watch next month, etc.
 * We will build a linear regression model to predict for each customer how many videos they are likely to watch in the coming months.
 * Our data is in vppChapterViews folder. This spans a five month period from Jan - June 2018. I couldnt retrieve all the data here so I included some parts so code can still compile.
 * This data was from a relational database and instructor used Spark to manpulate the data into the format presented to us so most work was done for us. 
 * We also have four separate csv files, this reflects that there are 4 partitions at the last data processing.
 * 
 * Lets talk through each data field:
 * payment_method_type - tells us if they paid by credit or debit card (sagepay) or paypal account. This is a string field. Categorical
 * country - country code also categorical
 * Rebill_preiod_in_months - this tells us if customer pays their subscription every month (1), 6 months, or 12 months. Values are ONLY 1,6, or 12. Treat this as categorical even tho its numerical
 * firstSub - will be 1 or NULL. 1 if this is customers first subscription. If customers had multiple subscriptions, this will be NULL. Treat as boolean.
 * Observation_date - date the record refers to. ex - 4/1/2018 means we are trying to predict their behavior in April 2018 so we are looking at their behavior in previous month, March 2018. 
 * It is suggested to IGNORE THIS FIELD.
 * age - num of days since customer started their subscription. FIne to treat as a number.
 * all_time_views - num of chapters customer viewed in total over all subscriptions. Treat as a number too. Note that a customer could have watch a chapter more than once, this variable
 * 	takes that into account. If you watch a single chapter 5 times, it will count as 5 all time views.
 * last_month_views - how many chapters they watched last month.
 * next_month_views - the value we are trying to predict, how many chapter do they watch the following month? THis is our label
 * is_cancelled - boolean, has customer cancelled their account. We are interested in customers where is cancelled is false. So we must filter that our first.
 * 
 * Our task: load up data and do some data prep. 
 * - Filter out records where customer cancelled accounts. 
 * - Next remove any NULLs. Field with potential NULLs are firstSub, all_time_views, last_month_views, next_month_views. Write code to change the NULLs to other values. Keep the records!
 * - Data prep done, build the model with linear regression. Use all fields EXCEPT observation date. We can investigate the fields if we want.
 * - Convert all categorical data into oneHotEncoded vectors. Use pipelines if we want. Dont use pipelines AT first so we can check the state of our dataset.
 * - use a range of model fitting parameters, we can choose which parameters we want to use. Use more than 3 to maximize likeligood of good value. Minimize r2 value
 * - when we train test split data, suggestion is to use 90/10  split for holdout and for test.
 * 
 * Take note that our model will NOT have a particularly good r2 value since the features we will be using are not necessarily the best features for predicting the label in the real world.
 * We use some other features which actually turn out to be quite good predictors but we want to keep customer information safe. Next chapter changes modle type which produces better results.
 * This challenge is actually suited better to a different model type.
 * 
 * For replacing NULLs with other values, we can overdo it with a user defined function. Little bit of code will be able to do it such as: 
 * 
 * myDataset = myDataset
 * 	.withColumn("myColumn", when(col("myColumn").isNull(), 0).otherwise(col("myColumn"));
 * 
 * when function takes two arguments, can use a boolean. If value in column is NULL, replace with 0. This is from the spark.sql.functions package.
 */
	
	public static void main(String[] args) { //boilerplate
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Chapter Views Case Study")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true)
				.csv("src/main/resources/vppChapterViews/part-r-00000.csv"); //for now taking one of the csvs 
	
	csvData = csvData.withColumn("firstSub", when(col("firstSub").isNull(), 0).otherwise(col("firstSub"))) //checking firstSub. If NULL, make it 0	
					 .withColumn("next_month_views", when(col("next_month_views").isNull(), 0).otherwise(col("next_month_views")))
					 .withColumn("last_month_views", when(col("last_month_views").isNull(), 0).otherwise(col("last_month_views")))	
					 .withColumn("all_time_views", when(col("all_time_views").isNull(), 0).otherwise(col("all_time_views")))
					 
					 .withColumnRenamed("next_month_views", "label"); //rename next month views because we are predicting this

	Dataset<Row>[] dataSplits = csvData.randomSplit(new double[] {0.9, 0.1}); //requirement to do 90-10 split
	Dataset<Row> trainingAndTestData = dataSplits[0];
	Dataset<Row> holdOutData = dataSplits[1]; 
	
	
	StringIndexer paymentIndexer = new StringIndexer(); //index and encode categorical data - payment_method_type, country, rebill_preiod_in_months   
	paymentIndexer.setInputCol("payment_method_type");
	paymentIndexer.setOutputCol("paymentIndex");
	
	StringIndexer countryIndexer = new StringIndexer();
	countryIndexer.setInputCol("country");
	countryIndexer.setOutputCol("countryIndex");
	
	StringIndexer rebillIndexer = new StringIndexer();
	rebillIndexer.setInputCol("rebill_preiod_in_months");
	rebillIndexer.setOutputCol("rebillIndex");
		
	
	OneHotEncoderEstimator encoder = new OneHotEncoderEstimator(); //do oneHotEncoder after indexer
	encoder.setInputCols(new String[] {"paymentIndex", "countryIndex", "rebillIndex"}); 
	encoder.setOutputCols(new String[] {"paymentVector", "countryVector", "rebillVector"});
	
//	csvData.show();
	
	VectorAssembler vectorAssembler = new VectorAssembler() //vector assembler, include everything except observation date
			.setInputCols(new String[] {"firstSub", "age", "all_time_views", "last_month_views", "is_cancelled", "paymentIndex", "countryIndex", "rebillIndex" })
			.setOutputCol("features"); //oops forgot to filter out records where customer has cancelled
	
	
//instantiating linear regression and param builder
	
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
	
	Pipeline pipeline = new Pipeline(); 
	pipeline.setStages(new PipelineStage[] {paymentIndexer, countryIndexer, rebillIndexer, encoder, vectorAssembler, trainValidationSplit});
	PipelineModel pipelineModel = pipeline.fit(trainingAndTestData);
	
	TrainValidationSplitModel model = (TrainValidationSplitModel)pipelineModel.stages()[5]; 
	LinearRegressionModel lrModel = (LinearRegressionModel)model.bestModel();
	
	Dataset<Row> holdOutResults = pipelineModel.transform(holdOutData);

	holdOutResults = holdOutResults.drop("prediction"); 

	System.out.println("Training data r2 value is " + lrModel.summary().r2() + " and the RMSE is " + lrModel.summary().rootMeanSquaredError()); 
	System.out.println("Testing data r2 value is " + lrModel.evaluate(holdOutData).r2() + " and the RMSE is " + lrModel.evaluate(holdOutData).rootMeanSquaredError()); 
	System.out.println("coefficients " + lrModel.coefficients() + " intercept: " + lrModel.intercept());
	System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam());

	
	}
}