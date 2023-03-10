package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

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

public class Lesson106_1 {

/* Lesson 106 Understanding Vectors - PART 1 Working on the gym competition example
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
		
		StringIndexer genderIndexer = new StringIndexer(); 
		genderIndexer.setInputCol("Gender"); 
		genderIndexer.setOutputCol("GenderIndex");
		csvData = genderIndexer.fit(csvData).transform(csvData);
		
		csvData.show();
		
/* CompetitorID | Gender | Age | Height | Weight | NoOfReps | GenderIndex
 * 1			| M	  	 | 23  | 180    | 88	 | 55       | 0.0
 * 4            | F      | 24  | 166    | 60     | 44       | 1.0
 */
		
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
		csvData.show();
		
/* CompetitorID | Gender | Age | Height | Weight | NoOfReps | GenderIndex | GenderVector
 * 1			| M	  	 | 23  | 180    | 88	 | 55       | 0.0         | (1,[0],[1,0])
 * 4            | F      | 24  | 166    | 60     | 44       | 1.0         | (1,[],[])
 * 
 * Remember we first mentioned vectors they are a bit like arrays. We can see the vector vs array. The spark designers COULD have chosen arrays but there is an issue.
 * Imagine working with a category with 20 different values. That array would have a size of 19. Most of the values would be 0. Now think we have 20k records of this example with huge
 * arrays almost all of which are 0s would take up so much memory. So isntead, the vector ia neater and memory efficient way to represent an array. Lets understand how to read the vector.
 * 
 * So lets keep the example simple, we have a feature with 5 possible values. Heres what it would look like:
 * 
 * Grade | Array     | Vector
 * A     | [0,0,0,0] | (4,[],[])
 * B     | [1,0,0,0] | (4,[0],[1])
 * C     | [0,1,0,0] | (4,[1],[1])
 * D     | [0,0,1,0] | (4,[2],[1])
 * E     | [0,0,0,1] | (4,[3],[1])
 * 
 * So look at this, building an array instead of a vector ends up with an array of size 4 with 5 possible values. Lets look at the vector but start at the bottom value:
 * The vector is of size 4, with a 0 in every position EXCEPT for 3 and the numbers that ARENT 0 are 1. So in other words: We have an array of size 4 with ALL 0s except there is a 1 in position 3.
 * Hope that makes sense. This is a neater syntax of writing an array. Think of if we had an array of 30 possible values, this would take up a lot less memory than the array.
 * 
 * Thats what vectors are really are. Now to go back to the gender vector we can see the vector of size 1 and sometimes its always 0. But for the males, it has a 1 in the first position.
 * Again, the GenderVector will be more useful when working with categories with a bunch of options such as house price analysis.
 */
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight", "GenderVector"} ); //added genderVEctor		
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
		
//		csvDataWithFeatures.show();
		
		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
		LinearRegression linearRegression = new LinearRegression();
		LinearRegressionModel model = linearRegression.fit(modelInputData); 
		System.out.println("The model has intercept " + model.intercept() + " and coefficients " + model.coefficients());

		model.transform(modelInputData).show();
		
/* ********** EXERCISE FOR ME - lets do this vector and add the extra features to the house price model for the variables Condition, Grade, Zipcode, and Waterfront.
 */

		
	}
}