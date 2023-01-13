package com.virtualpairprogrammers;
//Created in Lesson 6

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		// basic spark app to test jar files work
		List<Double> inputData = new ArrayList<Double>();
		
		inputData.add(35.5); 
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);
		
//removing logging. Make sure to use the following for the logger import: org.apache.log4j.Logger
		Logger.getLogger("org.apache").setLevel(Level.WARN); //filter out apache loggers. set level so we only see warnings. For WARN make sure to select the org.apache.log4j.Level Run again to see results
		
//RDD - resilient distributed dataset, we will sue this to distrubute our data across multiple computers in a cluster
//spark boilerplate
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); //represents configurations of spark. CTRL SHIFT O after running maven to import sparkCOnf

//this app name^^ will be shown in reports. We will go over setMaster later in the course. We are saying use spark locally, no cluster. * means use all available cores on machine to run this program
//if we just wrote local, we would have the program run on a single thread, wont get full performance
		
		JavaSparkContext sc = new JavaSparkContext(conf);//represents a conenction to our spark cluster but will allow us to communicate with spark
		
//load some kind of data into spark. we will use our local input for now.
		JavaRDD<Double> myRdd = sc.parallelize(inputData); //this loads in a collection and turns it into an RDD
		
//JavaRDD is java representative of an RDD. Can use java syntax. Under the hood, this communicates with a Scala RDD. THink of this as a wrapper. 
//javaRDD is generic so we must specify with double angle brackets what kind it is <>
//for spark context we are supposed to close it when our program is finished. last line should be sc.close()
//right click main.java, run as java program. The red text in the console is jsut logging information. 
		
		
		
		
		
		sc.close();
	}

}
