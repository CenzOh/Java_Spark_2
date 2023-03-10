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
		
		List<Integer> inputData2 = new ArrayList<>(); //making a new list of integers for map fcn
		inputData2.add(35); 
		inputData2.add(12);
		inputData2.add(90);
		inputData2.add(20);
		
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
//right click main.java, run as java program. The red text in the console is the logging information. 

//Lesson 7 We will learn about the RDD Reduce function. THink Hadoop or Java 8 lambdas. Use the reduce against an RDD to transform big dataset into one single answer
//we want to sum up the values that is contained. We will be doing this across a cluster, so doing it with a count variable will not work. Ex - Node 1 - 7, 4, 9. Node 2 - 8, 2, 7...
//with reduce we define a function (reduce fcn lets call it) summation will be simple, function = value1 + value2. The driver (VM running the program) can send the fcn to each partition (or node)
//return type of reduce fcn must be same type as input, thats really the only downside of it
		
		Double result = myRdd.reduce(( value1,value2 ) -> value1 + value2);
		
//this is a block of code we want to send to the partitions. We are saying, input 2 values, then we want to add them together. How does it know the types? We can declare them if we want
//but because java knows RDD is working on obejcts of type double, it already knows these are doubles. Can be inferred.
//run the program, we will disucss later what `Stage 0` means. WE
	
		System.out.println(result); //output says 158.63943
		
//Lesson 8, mapping fcn. Our example will be making a sqrt fcn. Map will allow us to define the fcn and it will be sent by the driver to each partition. 
//to store the values, we cant modify existing order since it is immutable. Instead, it creates a new RDD and will be populated. RDD 35 --> sqrt(value) --> RDD 5.91608

		JavaRDD<Integer> myRdd2 = sc.parallelize(inputData2); //making new RDD to take in input of integers
		
		JavaRDD<Double> sqrtRdd = myRdd2.map( value -> Math.sqrt(value)); //map needs to take one input param of type int (input data) and returns any type we like. Use reduce fcn as an inspiration
//in sqrt fcn the input has to be double, but our value will be auto configured to be a double even though its an int
//once an RDD is created it is immutable, cant be cahnged, map will return a NEW RDD. It is type JavaRDD and instead of generic type we can specify it is holding Doubles (not integers after conversion)

//Lesson 9 output results. Common result is to write data in a file but what if the data is too big? We can write to a file system like HDFS and distribute across cluster.
//for testing purposs let us use the for each method which will call every element. Not same as map. This calls a procedure, doesnt make an RDD, a void funciton.

//		sqrtRdd.foreach( value -> System.out.println(value)); //first version
//		sqrtRdd.foreach( System.out::println ); //another way to do the same as above. New thing in java 8. Because single value being treated in each element, we dont need to give a name
		sqrtRdd.collect().forEach( System.out::println ); //fixed version. Explaination at botton. add collect() method and change foreach to be java version which is forEach. 
		
/* OUTPUT:
9.486832980505138
5.916079783099616
3.4641016151377544
4.47213595499958
 */
		
//Lesson 10 big data patterns. lets say we want to see how many elemtns are in the sqrtRdd. We can simply use the method count
		System.out.println(sqrtRdd.count()); //prints 4. This is ok for the simple example but further with spark we want to keep the count inside an RDD to do further transformations.

//count using just a map and reduce. Use the map process and map every value in the RDD to out value of 1. Then use reduce to add up all the vlaues in the second RDD.
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L); //map fcn to turn every value into `1` in the new RDD
		Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2); //use the reduce function to count instances (how many) are in the new RDD
		System.out.println(count); //prints 4
		
//Lesson 11 fix for non serializable. For the print out statement with the colons, this causes the error. Essentially what the error is is, Spark is trying to serialize the method to send to our
//multiple CPUs. So the print line method in standard java dev kit is non serializable, so it cant be passed in a serialization routine. Not an error with one physical cpu (even with multi cores)
//simple fix is this: add the method call into the chain, add the collect method. THis will take all the data in our RDD (even if distributed) and gather the data together onto the JVM which we are running the code.
//it will return a regular java list. so now we wont have an RDD so we can do a regular loop, will work because no need for serialization.
		
		
		
		
		sc.close();
	}

}
