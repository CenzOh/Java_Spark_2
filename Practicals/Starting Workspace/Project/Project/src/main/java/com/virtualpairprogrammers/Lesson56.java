//Lesson 55 and 56
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Lesson56 {
	
	public static void main(String[] args) {

	
		//Lesson 56 Intro to Spark SQL. Code given to us in Lesson 55
/* This is an alternative / newer syntax to use Spark instead of using RDDs. Spark SQL is designed to make it easier to work with structured data. THis d/n mean we will work with databases
 * We can work with DBs, which we will look at later. Spark SQL works with any format. In particular, big text files we used in first part of the course works fine.
 * Whats different about Spark SQL? It is a rich API to work with structured data. IE - data source, whatever format it is, contains what we think of as records and each record
 * contains a known set of fields. Ex - comma separated value like so:
 * 
 * id, name, address, creditScore
 * 1, Bill Jones, 19 Carp Street, 42
 * 2, Belinda Fish, 98 Autumn Terrace, 23
 * 3, Kelly Southfield, 21 The Dale, 98
 * 
 * Looks like customer data. We have a header at top and we have fields. This is what we mean with structured data. We can process the data with RDDs like earlier. We can make a standard
 * Java class to represent a customer and the rest can be attributes of the customer (id, name, etc). We didnt do this very much, but we can build a java RDD containing customer objects of values
 * or we can use a Tuple which we used a lot of Tuple2s. Can have a lot of fields in a Tuple. So we can have a java RDD containing Tuple4s. But thats a lot of work to convert from one form
 * to another.
 * 
 * This new Spark SQL API is MORE flexible. Spark SQL gives us two complementary models. We can interact the data with syntax similar to SQL. THis is great to focus on WHAT we want from data
 * rather than HOW we implement algorithms. Useful for everyone really. Why not put this data in a database to use regular SQL? Well its because of size. We will be scaling up to big data.
 * Again we are talking about structured big data that CAN NOT fit into a single relational database.
 * 
 * ANother way to interact with Spark SQL is with an API called Datasets API. THis looks like standard java code. Also known as DataFrame API. WE will talk about difference at some point.
 * Dataset API is java api to manipulate and query structured big data. Note that RDDs we talked about last section are still relevant. If we have non structured data like what we looked
 * at before with log messages looking for keywords, (single field) that can work better as non structured data. In real world, a lot of data is structured. 
 * You need Spark SQL and RDD, so think of them as complementary, good to learn both.
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.close();
	
	}
	
}
