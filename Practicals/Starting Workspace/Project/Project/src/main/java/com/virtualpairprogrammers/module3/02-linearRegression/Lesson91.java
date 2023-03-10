package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson91 {

/* Lesson 91 Beginning Code Linear Regression
 * 
 * Lets come up with some predictions for this example. Instructor goes through tutorial to create new Java Project, remember the name should match. SparkModule3. Project should be found.
 * Currently we have two things, resources has the CSV file and the pom.xml. The pom is important bit with the dependencies. So far in the pom we have the three dependencies
 * for spark. Spark core, SQL, hadoop HDFS. We need new depednency:
 * 
 	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-mllib_2.11</artifactId>
		<version>2.3.1</version>
		<scope>runtime</scope>
	</dependency>
	
 * This is the spark ML Lib package dependency. Some changes to spark ML Lib have changed so we should use 2.3 and above. The version we pick MUST match spark core and spark sql which
 * are 2.11 in our dependencies. THat is why it was recommended to make a new project. Run the pom to get the dependencies. Run as > Maven Build > goal Eclipse:Eclipse
 * Look for the build SUCCESS message > Refresh the project. Make sure we have the GymCompetition.csv. We have twenty rows in the file. Lets load it into a dataset. Instructor created new class
 * called GymCompetitors I will ahve each lesson in a separate java file per usual.
 */
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); //set the hadoop home for windows
		Logger.getLogger("org.apache").setLevel(Level.WARN); //disable logging we dont want to see, set the level to WARN
		
		SparkSession spark = SparkSession.builder() //create spark session, pretty much boilerplate
				.appName("Gym Competitors") //name our application
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/") //config for windows
				.master("local[*]") //set master to use all local cores
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read().option("header",  true).csv("src/main/resources/GymCompetition.csv");
//dataset of rows is a DF recall. Put in option that there are headers to read. Then .csv() to read in the csv file with the path.
		
		csvData.show(); //checking if worked okay.

/*
 * CompetitorId | Gender | Age | Height | WEight | NoOfReps
 * 1 | M | 23 | 180 | 88 | 55
 * ...
 * 
 * It worked we are now ready to begin.
 */
				
	}
}
