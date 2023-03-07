package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorsClustering {

	
	
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
				.csv("src/main/resources/GymCompetition.csv");
		
		StringIndexer genderIndexer = new StringIndexer(); 
		genderIndexer.setInputCol("Gender"); 
		genderIndexer.setOutputCol("GenderIndex"); 
		csvData = genderIndexer.fit(csvData).transform(csvData);
				
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
		csvData.show();

/* CompetitorID | Gender | Age | Height | Weight | NoOfReps | GenderIndex | GenderVector
 * 1			| M		 | 23  | 180    | 88     | 55       | 0.0         | (1,[0],[1.0]
 */
		VectorAssembler vectorAssembler = new VectorAssembler();
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"GenderVector","Age","Height","Weight","NoOfReps"} ) 
			.setOutputCol("features").transform(csvData).select("features"); 
		inputData.show();
		
/* features             |
 * [1.0,20.0,185.0,7... |
 */
		KMeans kMeans = new KMeans(); 



//		kMeans.setK(3);
	
		
		for (int noOfClusters = 2; noOfClusters <= 8; noOfClusters++) { 
			
			kMeans.setK(noOfClusters); 
			System.out.println("No of clusters " + noOfClusters); 

			KMeansModel model = kMeans.fit(inputData); 
			Dataset<Row> predictions = model.transform(inputData);
			predictions.show();

/* features            | prediction
 * [1.0,23.0,180.0,8...| 2
 */
			Vector[] clusterCenters = model.clusterCenters(); 
			for (Vector v : clusterCenters) { System.out.println(v); } 
		
/* Cluster | Gender | Age  | Height | Weight | No of Reps
 * 0       | 0.333  | 22.0 | 172.0  | 71.333 | 50.0
 * 1       | 0.0    | 22.5 | 167.75 | 60.0   | 44.75
 * 2       | 0.8    | 22.1 | 179.0  | 83.60  | 54.30

 */
		
			predictions.groupBy("predictions").count().show();

 /* prediction | Count
 *  1          | 4
 *  2          | 10
 *  0          | 6
 */
			System.out.println("SSE us " + model.computeCost(inputData) );
		
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
			System.out.println("Silhouette with squared euclidean distance is " + evaluator.evaluate(predictions));
		}
	}
}