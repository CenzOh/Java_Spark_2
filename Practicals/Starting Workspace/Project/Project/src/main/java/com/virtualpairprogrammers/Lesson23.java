//Lesson 23
package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2; 

public class Lesson23 {

	public static void main(String[] args) {
		
//Lesson 23 Working Solution
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		initialRdd
			.flatMap( value -> Arrays.asList(value.split(" ")).iterator())
			.foreach(System.out::println);
				
		sc.close();
	}

}
