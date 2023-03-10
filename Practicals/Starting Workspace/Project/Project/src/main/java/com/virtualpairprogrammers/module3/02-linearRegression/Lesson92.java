package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson92 {

/* Lesson 92 Assembling a Vector of Features
 * 
 * First job, manipulate our data to be in right format for spark ML to use as input data for the methods to fit a good model for supervised learning! THis is what the format should look like:
 * 
 * label | features
 * 55 | {23, 180, 88}
 * 54 | {18, 169, 84|
 * ...
 * 
 * So recall the format is a dataset with two cols. For supervised models we have One label and one features. We can have extra cols but THEY WILL be ignored. Label column MUST be numeric
 * and will contain the num we are trying to predict (num of reps) / known outcomes in source data. Features is an array of values we want to feed in (weight, age, and height in our case).
 * We called the features an array but the idea is that it is an array of calues weight, height, age. Recall we are not including gender yet since right now all features must be num.
 * Also note that these features are NOT an array they are a special spark type called a Vector. Think of it like an array and we will see what makes it different later. DOnt be confused
 * with java object Vector. NOT THE SAME.
 * 
 * TO create a vector we need to use a helper object called Vector Assembler. The way it works is, we instantiate one, set two properties of our assembler. FIrst property is input cols
 * which will be an array of strings, each string being the name of the col in the dataset (one of our features).
 * Second property is output col, the name we will give to our vector once it has been created and will normally be the word features, which will be a string.
 * Once both are set, we can call its transform method passing in the dataset we will work with. Vector Assembler will add in the new col given the name for output col and the value will
 * be a vector containing the values for each of our input cols. Lets do it so it would make sense.
 */
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
//		Dataset<Row> csvData = spark.read().option("header",  true).csv("src/main/resources/GymCompetition.csv"); //FIXING NEXT LINE
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true) //telling spark to work out current data type of each col. SPark doesnt always get it right so we would have to do casting in that case.
				.csv("src/main/resources/GymCompetition.csv");
		
		csvData.show(); 
		
//every time we import an object in spark ensure it comes from this ML package. If we import from MLib, this will be the OLDER version designed to work with spark RDDs.
		VectorAssembler vectorAssembler = new VectorAssembler();
		
//set two properties, first is input cols. Takes in an array of string which again, are the col names we are feeding in. We are creating a vector out of these three features
		vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight"} );
		
 //output col is just the name which we want to call it features
		vectorAssembler.setOutputCol("features");
		
//pass in the csv file. Transform returns a new dataset
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
		csvDataWithFeatures.show();
		
/* Instructor suspects this will NOT WORK. We received an error. ANd thankfully it is readable. It says `Data type StringType of columns Age is not supported`. THis means our age col is a
 * string. FOr VA to work, every value in the features must be a number. Lets fix. Easiest way is to infer the schema.
 * 
 * Lets print the schema so we can see if it works:
 * root
 * |-- CompetitorID: integer (nullable = true)
 * |-- Age: integer (nullable = true)
 * ...
 * 
 * Good it worked all our input features are ints. LEts see the new features cols too:
 * CompetitorId | Gender | Age | Height | WEight | NoOfReps | features
 * 1 | M | 23 | 180 | 88 | 55 | [23.0,180.0,88.0]
 * ...
 * 
 * One last thing to get everything in right format. Take NoOfReps and rename it to the label. Lets get rid of the other cols too.
 */
		csvDataWithFeatures.printSchema();
		
//here we are taking our csvWithDataFeatures, selecting our two cols, renaming the No of reps col to label and saving it into another DF.
		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
/* label | features
 * 55 | [23.0,180.0,88.0]
 * ...
 */
	}
}