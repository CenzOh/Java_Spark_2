//Lesson 120
package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VPPCourseRecommendations {

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
				.csv("src/main/resources/vppCourseViews.csv");
		
		csvData.show();
/* userID | courseID | proportionWatched
 * 1	  | 1		 | 0.15
 */
		
		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
		csvData.show();

		csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show();
		
/* userID | 1      | 2       | ...
 * 496	  | NULL   | 62.0    | 
 * 471	  | 28.0004| 74.0    |
 */
		Dataset<Row>[] trainingAndHoldoutData = csvData.randomSplit(new double[] {0.9, 0.1});
		Dataset<Row> trainingData = trainingAndHoldoutData[0];
		Dataset<Row> holdoutData = trainingAndHoldoutData[1];
		
		ALS als = new ALS()  
				.setMaxIter(10) 
				.setRegParam(0.1) 
				.setUserCol("userId")
				.setItemCol("courseId")
				.setRatingCol("proportionWatched");
		
		ALSModel model = als.fit(trainingData); //import ALSModel
		
		Dataset<Row> predictions = model.transform(holdoutData);
		predictions.show();
		
/* userId | courseId | proportionWatched | prediction
 * 53	  | 12		 | 12.0 			 | 107.843
 * 155	  | 12		 | 100.0			 | 91.8167
 * 76	  | 12 	 	 | 100.0			 | 71.8678
 */
		ALSModel model1 = als.fit(csvData);
		
		Dataset<Row> userRecs = model1.recommendForAllUsers(5);
		userRecs.show();
/* userId | recommendations
 * 471	  | [[16, 94.4563], ...
 */
		
		List<Row> userRecsList = userRecs.takeAsList(5); 
		
		for (Row r : userRecsList) {
			int userId = r.getAs(0);
			String recs = r.getAs(1).toString(); 
			System.out.println("User " + userId + " we may want to recommend " + recs);
			System.out.println("This user has already watched: " );
			csvData.filter("userId = " + userId); 
/* userId | courseId | proportionWatched
 * 471    | 1        | 28.000004
 */
			Dataset<Row> testData = spark.read()
					.option("header",  true)
					.option("inferSchema", true)
					.csv("src/main/resources/vppCourseViewsTest.csv");
			
			ALSModel model2 = als.fit(csvData);
			
			model2.setColdStartStrategy("drop"); 
			
			model2.transform(testData).show();
			model2.recommendForUserSubset(testData, 5).show();

/* userId | courseId | proportionWatched | prediction    // this is fomr the model2.transform().show() line
 * 601    | 21       | 0.5               | NaN
 * 
 * userID | recommendations    // for the model2.recommendForUserSubset().show()
 * 
 */
		}
	}
}
