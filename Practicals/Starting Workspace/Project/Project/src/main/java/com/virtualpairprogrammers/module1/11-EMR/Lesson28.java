//Lesson 28
package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Lesson28 {
	
	public static void main(String[] args) {

	
		//Lesson 28-32 AWS EMR optional section, will document some steps
/* Lesson 28 How to start EMR Spark Cluster
 * We will be running our Spark app on a real hardware cluster using Amazon EMR. Will be documenting how to do this. Assuming account for AWS already created and familiarity with AWS.
 * Even if you dont have an account, this will be good for insight. Everything done in the chapter will incur cost. If failed to delete or terminate resources you will be billed!
 * Objective - deploy keyword count app to live hardware. Basically we need cluster manager. Spark supports hadoop cluster manager. Hadoop is NOT just map reduce. It is collection
 * of tools including map reduce but also HDFS file system and cluster manager. If setting up cluster by hand, we can set up a cluster in the same way that we would set up a hadoop cluster.
 * Can also use existing AWS service called EMR. EMR stands for Elastic Map Reduce, this is amazons implementation of hadoop in the cloud.
 * 
 * On AWS site go to Amazon EMR page. Under Clusters tab select button to `create cluster`. Give it name like `Video Demonstration Cluster`. DOnt need logging untick this.
 * If left ticked, some logs would be written to an S3 folder. Untick because we will be charged. Two options for launch mode. If we launch as cluster, we will end up with
 * a set of Amazon EC2 instances that we can log onto and control programatically (full control of cluster). With step execution, we can configure things so that the cluster start sup
 * executes a job or series of jobs and then cluster auto terminates. Very useful for tried and test job that we want to run at certain time and date.
 * Recommended to NOT go for step execution even with issue with code (we will have to debug) because entire cluster will shut down then ahve to start it up again.
 * 
 * 
 * Lesson 29 packing a spark jar for EMR
 * Need to make a few changes to code. First changed our code back to have sortByKey and print to console:
 * JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
 * results.forEach(System.out::println);
 * 
 * This works locally but some issues. FIrst when calling .setMaster() we tell hadoop to run in local mode. Asterisk tells hadoop to run this local multi thread program using as
 * many threads that are available. Will not work well if we leave this in a place where we deploy to a cluster since it would make it run JUST on driver and executive nodes will
 * do nothing. Nothing complicated just remove .setMaster(). In text file, we read in from local resources will NOT be available on cluster.
 * Have to copy file across cluster to have it as local, painful to do since textilfe operation doesnt work well with files stored inside a jar. Instead make file stored on S3 URL
 * sc.textFile("s3n://vpp-spark-demos/input.txt");
 * Distribute to cluster, in pom.xml check name of <mainClass> may have different package and main class. If in place we are good. Build as jar file, run pom.xml, select goal to be package.
 * In real life we would have this integreated into jenkins or something similar but for now manual upload. 
 * 
 */
	
		System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
	
		sc.close();
	
	}

}
