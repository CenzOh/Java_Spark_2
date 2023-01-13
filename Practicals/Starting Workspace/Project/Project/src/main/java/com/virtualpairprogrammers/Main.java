package com.virtualpairprogrammers;
//Created in Lesson 6

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		// basic spark app to test jar files work
		List<Double> inputData = new ArrayList<Double>();
		
		inputData.add(35.5); 
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);
//RDD - resilient distributed dataset, we will sue this to distrubute our data across multiple computers in a cluster
//spark boilerplate
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); //represents configurations of spark. CTRL SHIFT O after running maven to import sparkCOnf

//this app name^^ will be shown in reports. We will go over setMaster later in the course. We are saying use spark locally, no cluster. * means use all available cores on machine to run this program
//if we just wrote local, we would have the program run on a single thread, wont get full performance
		
		JavaSparkContext sc = new JavaSparkContext(conf);//represents a conenction to our spark cluster but will allow us to communicate with spark
		
//load some kind of data into spark. we will use our local input for now.
		sc.parallelize(inputData); //this loads in a collection and turns it into an RDD
		
		
		
		
		
	}

}
