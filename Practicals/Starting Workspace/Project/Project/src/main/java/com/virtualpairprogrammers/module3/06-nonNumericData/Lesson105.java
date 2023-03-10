package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson105 {

/* Lesson 105 Usiong OneHotEncoding
 * 
 * Up to this point we have been working with numeric data because the mathematical algorithms ML uses requires our features to be features.
 * Lets try to use non numeric. First lets go back to the simple data set of gym members and how many reps they can perform
 * 
 * CompetitorID | Gender | Age | Height | Weight | NoOfReps
 * 1			| M	  	 | 23  | 180    | 88	 | 55
 * 
 * Recall that we excluded the gender field. Now, lets include gender in our model build. Spark gives us a way to automate to make it work. Lets look at theory first:
 * We need to give our gender field a value. Not as simple as M = 1, F = 2, unknown = 3. This d/n work because having values like this may show higher order or rank.
 * This is not true. The algorithm will think Females do better than males and unknowns do better than females. Makes no sense. Spark will look for correlation between NoOfReps and gender.
 * 
 * So we want to convert gender to a num and avoid an implied rank. DO this by converting to three separate fields. So do it like this:
 * CompetitorID | Male | Female | Unknown | Age | Height | Weight | NoOfReps
 * 1			| 1    | 0      | 0       | 23  | 180    | 88	 | 55
 * 
 * Now we can see if competitor is a male, 1 in male and 0 in rest. This split removes bias and ranking. Model building process will show that the data point has that particular feature
 * or not. Now we will get meaningful outcome. We made these fields into a boolean value. 
 * 
 * SO in spark we dont have to go to this level of separate fields. We can combine these values into a single field by making it a vector:
 * CompetitorID | Gender | Age | Height | Weight | NoOfReps
 * 1			|[1,0,0] | 23  | 180    | 88	 | 55
 * 
 * So we combine the three values where only one of them will be a 1. Will work fine! Spark does allow us to have a vector instead of a number. So our features and labels will end up like:
 * label | features
 * 55    | [[1,0,0],23,180,88]
 * 
 * These vectors can represent a single field. This is what we want to get to. But not the exact end point, will look at that shortly. 
 * Again, spark can autoamte some of this process for us with indexing and encoding stages. First it indexes, so it asigns a unique number to each value and start with 0.
 * Ex: M - 0, F - 1, U - 2. Second process is encoding and then it converts to a vector. Ex: M - [0,0], F - [1,0], U - [0,1]. We need a vector of size 2 since we can represent these
 * three values by having no ones, or one on the first point or second point. Spark will create a vector which is more efficient than what we did since the vecotr size will be
 * smaller than max num of possible values. 
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
				.csv("src/main/resources/GymCompetition.csv");
		
//		csvData.printSchema(); //to review the schema again
		
		StringIndexer genderIndexer = new StringIndexer(); //import string indexer from ml library, create an index and set which col we want to index on in our case is gender
		genderIndexer.setInputCol("Gender"); //which col to idnex on
		genderIndexer.setOutputCol("GenderIndex"); //name of new col
		csvData = genderIndexer.fit(csvData).transform(csvData);
		
		csvData.show();
		
/* CompetitorID | Gender | Age | Height | Weight | NoOfReps | GenderIndex
 * 1			| M	  	 | 23  | 180    | 88	 | 55       | 0.0
 * 4            | F      | 24  | 166    | 60     | 44       | 1.0
 * 
 * We have our new gender index col, it have 0 to males and 1 to females. Now lets turn it into a vector with the encoding stage. Lets use OneHotEncoderEstimator. (oneHotEncoder deprecated 
 * as of spark 2.3) 
 */
		
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator(); //this works similarly to index with input col, output, fit, transform. THis does it multi cols.
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
		csvData.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight"} );		
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
//output next lesson
		
//		csvDataWithFeatures.show();
		
		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
		LinearRegression linearRegression = new LinearRegression();
		LinearRegressionModel model = linearRegression.fit(modelInputData); 
		System.out.println("The model has intercept " + model.intercept() + " and coefficients " + model.coefficients());

		model.transform(modelInputData).show();
	}
}