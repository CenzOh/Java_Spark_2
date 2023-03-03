//Lesson 114 - 116
package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class VPPFreeTrial {


	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

		@Override
		public String call(String country) throws Exception {
			List<String> topCountries = Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries = Arrays.asList(new String[]
					{"BE","BG","CZ","DK","DE","EE","IE","EL","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});

			if (topCountries.contains(country)) return country;
			if (europeanCountries.contains(country)) return "EUROPE";
			else return "OTHER";
		}
	};
	
	
	public static void main(String[] args) {
	
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("VPP Chapter Views")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
		spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);
		
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true)
				.csv("src/main/resources/chp10data/vppFreeTrials.csv");
		
		csvData = csvData.withColumn("country", callUDF("countryGrouping", col("country")) ) 
				.withColumn("label",  when(col("payments_made").geq(1), lit(1)).otherwise(lit(0)));
		
/* payments_made | label | ...
 * 1			 | 1
 * 2			 | 1
 * 0			 | 0
 * ...
 */
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country")
					  .setOutputCol("countryIndex")
					  .fit(csvData).transform(csvData);
		
		csvData.show();

/* country | countryIndex
 * UNKNOWN | 3.0
 * US	   | 0.0
 * GB	   | 5.0
 * ...
 */
		Dataset<Row> countryIndexes = csvData.select("countryIndex").distinct(); 
		countryIndexes.show();

/* countryIndex |
 * 0.0          |
 * ...
 */
		
		IndexToString indexToString = new IndexToString(); 
		indexToString.setInputCol("countryIndex").setOutputCol("value").transform(countryIndexes).show(); 
		
		
/* countryIndex | value
 * 0.0 			| US
 * 1.0 			| OTHER
 * 4.0 			| EUROPE
 * 3.0 			| UNKNOWN
 * 2.0 			| IN
 * 5.0 			| GB
 */
		new IndexToString()
			.setInputCol("countryIndex")
			.setOutputCol("value")
			.transform(csvData.select("countryIndex").distinct())
			.show();
		

		VectorAssembler vectorAssembler = new VectorAssembler(); //instantiate
		vectorAssembler.setInputCols(new String[] {"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"}); //col of our features
		vectorAssembler.setOutputCol("features"); //has to be called this for our model
		
		Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label", "features");
		inputData.show();

/* label | features
 * 1     | [3.0,1.0,3.0,9406.0]
 * 0     | [0.0,1.0,7.0,1354.0]
 * ...
 */
		Dataset<Row>[] trainingAndHoldoutData = inputData.randomSplit(new double[] {0.8, 0.2}); 
		Dataset<Row> trainingData = trainingAndHoldoutData[0];
		Dataset<Row> holdoutData = trainingAndHoldoutData[1];
		

		DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier();
		dtClassifier.setMaxDepth(3);
		
		DecisionTreeClassificationModel model = dtClassifier.fit(trainingData); 
		
//		model.transform(holdoutData).show(); 
		Dataset<Row> predictions = model.transform(holdoutData);
		predictions.show();
				
/* label | features            | prediction
 * 0     | [0.0,1.0,3.0,586.0] | 0.422222
 * 0     | [0.0,6.0,4.0,0.0]   | 0.733333
 */
		System.out.println(model.toDebugString());
		
/* DecisionTreeRegressionModel (uid=dtr_2267...) of depth 3 with 15 nodes
 * 	If (feature 3 <= 9886.5)
 * 		If (feature 2 <= 5.5)
 * 			If (feature 3 <= 1887.0)
 * 				Predict: 0.2528
 * 			Else (feature 3 > 1887.0)
 * ...
 */
		
/* ******* START HERE *************
 * In this instance we want to setup an evaluator, rather than regression evaluator for classifier its multiclass classification evaluator. 
 */
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		System.out.println("accuracy level : " + evaluator.evaluate(predictions) ); 
/* 0.7233009
 * Our model accuracy not perfect but not bad first attempt.
 */
		RandomForestClassifier rfClassifier = new RandomForestClassifier();
		rfClassifier.setMaxDepth(3);
		RandomForestClassificationModel rfModel = rfClassifier.fit(trainingData);
		Dataset<Row> predictions2 = rfModel.transform(holdoutData);
		predictions2.show();
		
		System.out.println(rfModel.toDebugString());
		
		System.out.println("accuracy level of foret model: " + evaluator.evaluate(predictions2) ); //using same evaluator
/* DecisionTreeClassificationModel (uid=dtc_370cc...) of depth 3 with 13 nodes
 * ...
 * accuracy of model 0.691764 //slightly worse this time
 * 
 * RandomForestClassificationModel (uid=rfc_dda5987...) with 20 trees
 * ... // Lots of trees printed out it says Tree 0 (weight 1.0), Tree 1 (weight 1.0), etc.
 * accuracy of forest model is 0.68627 //We built up 20 different trees and accuracy is slightly lower. This instance not the best to use.
 * 
 * There are a lot of issues using random forest unfortunately. We will end up with a large num of possible trees, can be a lot of code to write in plain java for other systems.
 * We can improve this model using more granualr country groupings. Maybe five levels of depth. 
 */
	}
}