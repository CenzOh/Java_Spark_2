//Lesson 21
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

public class Lesson21 {

	public static void main(String[] args) {
		
//Lesson 21 Reading from Disk
//so far we have been working with hard coded data from a java collection. Good for spark fundamentals
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); //i explain this later in the code
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405"); 
		inputData.add("ERROR: Tuesday 4 September 0408"); 
		inputData.add("FATAL: Wednesday 5 September 1632"); 
		inputData.add("ERROR: Friday 7 September 1854"); 
		inputData.add("WARN: Saturday 8 September 1942"); 

		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
//load in an RDD and initialize it with data inside a text file. There is a method for this with the text file
		
/*NOTE - if doing this in production or on real big data set, this will NOT be a local text file on this machine since input data will be too big to fit on a single machine disk
 * often when working with this, we point to a distributed file system. Ex - is amazon distributed file storage system "s3://". Note that when we load in one of these files
 * is that it will NOT load the text file into memory on this virtual machine, otherwise we would have an out of memory exception. Driver will instead tell the nodes in the cluster
 * to load in parts / partitions. All this is contained inside the logic of text file. We will use a local file for now.
 * Refer to source/main/resources for different folders of pieces of test data to use we will work on the subtitles folder. Load in file, split file up into partitions as 
 * represented by this new RDD look at input data
 */
		
//		sc.parallelize(inputData) //sc parallelize only works for processing data in memory
		initialRdd
			.flatMap( value -> Arrays.asList(value.split(" ")).iterator())
//		.filter(word -> word.length() > 1) //unneeded
			.foreach(System.out::println);
		
/* simply get out each individual word from the subtitle file and print each one out. In this chapter, lets just check if loading in text file loading possible error is 
 * windows not being able to load winutils. Even tho the exception happened, the program still runs. Doesnt matter for us since we arent using hadoop
 * some of this data is dirty (some have , so it doesnt get split because we ask to split on space)
 * we can get rid of the exception by using windows compilation of hadoop binaries with winutils.exe can retrieve from github repo by `steveloughran`
 * we should already have this under `winutils-extra` folder. Then the path is hadoop > bin > winutils.exe.
 * copy the folder to somewher on the C drive (can put in root of c drive) again not a full hadoop but the winutils. setup the env variable to point to that folder
 * or can set up the enviornment in the program. Check above where I write System.setProperty(...
 */
				
		sc.close();
	}

}
