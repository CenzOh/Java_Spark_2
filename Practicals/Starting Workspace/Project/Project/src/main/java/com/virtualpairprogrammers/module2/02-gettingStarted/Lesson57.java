package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson57 {
	
/* Lesson 57 SparkSQL getting started
 * We have been previously working on various experiments in Main.java. For this section we will modify the code in that section (I will still put code in different java files)
 * for easier readability. There is another starting workspace with up to date code in case we skipped the first section and jumped straight into this section. 
 * In section 1, one of the joys of working with spark is that there are not a lot of dependencies that we need / no need for specific software. We simply have a maven pom.xml file
 * and we can see in there the dependencies are basic, spark core, spark SQL (we added previously for this part of the course), and apache hadoop (access HDFS file system).
 * 
 * In case you skipped first section, what we have to do is run the pom.xml as a maven build, goal is `eclipse:eclipse` so eclipse can download all the jar files required for spark.
 * The previous versions we were given was version 2.0 which is an old version of Spark. As of this video, you can check the apache spark page and see there are a lot of current versions
 * latest during the recording is spark 2.3.2. As of when I am writing this we can use Spark 3. Would make sense to upgrade these versions, you can easily do that in the pom.xml file 
 * just change <version>2.3.2</version> under spark core dependency and sql dependency. HOWEVER, issue of doing our latest version is we may have variances between
 * what happens in our development enviornments and what happens on video. Recommended to stick to 2.3.2 since recording has that.
 * 
 * The complexity of this is that the artifact IDs for both core and sql there is a version num on the artifact id. It has .10. On spark download page, we can see that `as of spark 2.0,
 * Spark is built with Scala 2.11 by default` thats what we are seeing here, the version of Scala. For spark 2.3.2, the artifact needs to be 2.11 for Scala version. Make sure to update that too 
 * under <artifactId>spark-core_2.11</artifact> Run the pom.xml again. Lots of download should occur. 
 * 
 * Now take note, may have happened to us, this was complaining that some jar files could not be written properly. To check if everythings okay, refresh project. Instructor received this
 * red X on the project. Look in our main or other files which should compile correctly. We may see there are all errors on the Spark objects. This could have been a corruption of
 * our Maven repo. Happens frequently when upgrading a library version. Easy way to fix this: to go home directory, find hidden folder called .m2 this is the maven cache. Delete this directory,
 * run the build again so it forces to download everything again. May take longer but it works.
 * 
 * Now we can go to our main class this is the section where we were experimenting with RDDs, now we will find how to convert this Spark SQL program. Need to work with structured data section. 
 * We can find this under the exams subfolder in resources. There is a CSV file inside called students.csv. Open it, may take a minute. Pretty big file about 60 MB. In our code we can pretty much 
 * start fresh so I'll write what we would have to do below:
 */
	
	@SuppressWarnings("resource") //boiler plate start
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
//		SparkConf conf = new SparkCOnf().setAppName("startingSpark").setMaster("local[*]");
//		JavaSparkContext sc = new JavaSparkContext(conf);

/* boilerplate end, pretty much all we need to begin! SparkConf and Spark Context lines we have seen with RDDs. All the work we did in RDD library was done in Spark Context. We will NOT use
 * them again but we can compare what we need to do to work in Spark SQL. Similar recipe. Just that end result will end up with SparkSession. This is how we will operate SQL.
 */
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
/* We can do all this in one fluent line of code. Do .builder() first. Then .appName will name the app, this is optional. Name will appear in performance analysis and logs
 * instead of doing .setAppName() we simply do .appName(). Same with master. Before it was .setMaster() and now we just call .master(). Specify num of cores like before.
 * One thing we need to add (windows specific) we will add on separate line, config method. In config() we have to pass in a parameter which is the directory of some temp files stored by spark sql
 * the dir does not have to already exist. Second param is this directory while first is the spark.sql.warehouse.dir. Finish with call to get or create method which returns the instance
 * of the spark session objects.
 * Next thing to do is point the spark session objects with the data we are working with. Do this with .read() but we have to also further tell spark what TYPE of data we are dealing with.
 * .csv() is for reading in csv files which is what we have. Other methods like JDBC, connect to database, .json(), .text() for text files. Pick .csv() for now. The argument for this is a
 * reference to the file we are reading so again, similar to previous section.
 */
		Dataset<Row> dataset = spark.read().option("header",  true).csv("src/main/resources/exams/students.csv");
		
/* There is ANOTHER thing we have to do! Review the structure of the data and we can see that we have a header line. HEaders are optional but advantage of having a header is that Spark SQL
 * can read the line. When we start manipulating data, we can refer to the data by the col header which is super useful. By default, spark will assume there is no header line.
 * We have to call the method before CSV. use the option method so we can set some options. One of them is called header, just a string and set to value of true: .option("header", true)
 * The resulting object we will get back from this is called a Data Set. A DataSet in Spark SQL is an abstraction that represents our data and think of the dataset like a table containing our
 * data. We have to declare the object we can use the quick fix, create local variable dataset. We will talk about what the generic is, you can tell the Dataset contains a series of rows.
 * When we do things like reading in data in an RDD, we are not actually reading the data. We are setting up an in-memory structure representing the data. ANd it is only when we reach an action
 * that the work needs to be done.
 * We need to go further with this data set before we can actually see anything interesting happening next time. There is a very useful method on datasets, was not in RDD version of spark
 */
		dataset.show();
/* DONT HAVE THE CSV SO HERE IS WHAT IT SHOULD SAY:
 * student_id | exam_center_id | subject | year | quarter | score | grade
 * 			1 | 			 1 |    Math | 2005 |       1 |    41 |     D
 * ...
 * 
 * .show() shows the top 20 rows. What if we want to get a count of how many rows are in the dataset? USe the .count() method
 */
		long numberOfRows = dataset.count(); //int doesnt work here, have to use long since we wil get a big number!!
		System.out.println("There are " + numberOfRows + " records"); //2,080,223 records WOW!! Fairly big data set to work with.
		
/* Real quick we did NOT think about what was going on with the count method under the hood. In real life we dont have to think about it but worth pausing to reflect.
 * In previous section with RDDs, we looked at operations like map reduce. They typically use a count on big data. Although we use this dataset which feels like an in memory object we are
 * still with big data, still working with RDDs. They are hidden away inside the dataset. We can call count and not think deeply about what goes on under the hood just know there is an RDD
 * underneath this. The implementation of this count could well be an operation like map reduce. In this API we do not need to know exactly how the count is implemented.
 * WE just know it will be implemented as distributed count across multiple nodes.
 */
		
//		spark.close(); //close method on spark session unsure why not compiling
	
	}
}
