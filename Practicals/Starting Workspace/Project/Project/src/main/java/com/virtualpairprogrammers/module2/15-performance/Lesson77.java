package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;


public class Lesson77 {
	
/* Lesson 77 Understand Spark UI for Spark SQL
 * We will talk about some performance. Earlier when we looked at RDDs in spark core, we went deep into the internals of spark to understand whats going on.
 * Now we have been working on data sets / data frames we are STILL using spark core. Underneath all the data sets are just RDDs. On a dataset we can just call the RDD method. See below.
 * 
 * Everything we discussed in the performance chapter STILL applies when working in Spark SQL. We can use the Spark UI to look at whats going on in a particular spark job and
 * general advice is the same like avoid unnecessary shuffles. HOWEVER, there is a difficulty with Spark SQL / downside since we are working with much higher level of abstraction.
 * 
 * The issue is (with the SQL syntax see the SQL reference block of code), although we use RDDs, we are not really using java code to build those RDDs.  We simply use the syntax to
 * DECLARE to spark the results we want. We are letting spark generate java code to satisfy the requirement. THis is a problem because under the hood, spark creates an RDD,
 * does the mapping, and at times it will have to do shuffles (big problem with the shuffles). When doing a group by, spark gathers together all the rows that have the same value
 * (not key in this case) for a particular column and it would have to do a shuffle. 
 * 
 * Point is, it can be harder to get an intuitive feel for whats going on under the hood when working with high level of abstraction. Also everything being said here does
 * apply to the spark SQL java API as we have been using in the code below. Although we are writing java code, to be clear, the java code we are writing is doing the EXACT same thing
 * that SQL syntax is doing. Its declaring to spark SQL the results that we want. When we run the code, Spark SQL has to go and generate actual java code to fulfill the requirement.
 * 
 * The java code will be map operations against an RDD. So which of the two versions that we have (the SQL commented out ver) and the live version of java api is better?
 * They are slightly different since we added a pivot on the java api. So which of the two are better in performance? Answer is that the current SQL version does not perform well!!
 * How can we make it better? Maybe changing the grouping? 
 * 
 * Everything we have been saying here is the same in databases, when we write SQL it is not always obvious the performance of the SQL. Usually have to do some analysis on the SQL
 * statements with a query plan. Lets look back at the spark UI to compare the performance. Recall we used java api to make the pivot table. THis is not possible in raw SQl.
 * As things stand, java and sql code we have will do things a bit differently so lets make the java api code match the sql code
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> dataset = spark.read().option("header", true).csv("Src/main/resources/biglog.txt");
		
// SQL reference
//		Dataset<Row> results = spark.sql
//			("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
//			+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); 
			
// Java API / DataFrame version
			
		dataset = dataset.select(col("level"), 
						  date_format(col("datetime"), "MMMM").alias("month"), 
					  	  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
		
//		dataset.rdd(); //can call rdd method, this returns a standard RDD object like the rdds we worked with earlier in the course
	
//		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
//		List<Object> columns = Arrays.asList(months); //dont need these for this lesson either
			
//		dataset = dataset.groupBy("level").pivot("month", columns).count(); //dont need this for this lesson
		
		dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum"); //making the java code as similar to what we have in the sql version
		dataset = dataset.drop("monthnum");
 
		dataset.show(100);
		
		Scanner scanner = new Scanner(System.in); //our way to let us view the spark ui
		scanner.hasNextLine();
		
/* Now visit localhost:4040 to see our performance for the java api version. Job ID 1 is second job. It has two stages therefore there HAS been a shuffle at some point with the group by.
 * Difference with here and the earlier sections is the block in stage 1 called `WholeStageCodegen`. We will see this appears in every stage. In general, most of what we see inside
 * the stages will be this whole stage Codegen block. 
 * 
 * Stage 1 WholeStageCodegen -> Exchange -> Stage 2 Exchange -> WHoleStageCodegen -> map -> takeOrdered
 * 
 * The WholeStageCodegen is the result of co-generation from the declarative java. We asked for a selection of the level column and a couple of data columns under the hood.
 * SQL generates the java code to fulfill that.
 * The wholeStageCodegen was added in spark 2.0. Instructor showing us on spark documentation the issue for wholeStageCodegen. Back in 2016 a major upgrade / categorized as an Epic.
 * https://issues.apache.org/jira/browse/SPARK-12795
 * 
 * This is incredibly deep you can probably base an entire career on learning the internals of the wholeStageCodegen. The description for it says the following:
 * 
 * Whole stage codegen is used by some modern MPP (Massive Parallel Processing ie Spark) databases to archive great performance. For Spark SQL we can compile multiple 
 * operator into a single Java function to avoid the overhead from materialize rows and Scala iterator. 
 * 
 * Lots of claims that this improves Spark SQL by up to ten times! Doesnt mean ten times faster than Spark RDD. We will look at difference shortly. What we mean is, the generation of the
 * java code required to support these requirements that we have given spark is ten times faster than they had in previous versions of Spark SQL. 
 * With the wholeStageCodegen the performance of these queries are not far off from what we would have gotten by doing all this by hand in low level RDDs. 
 * 
 * Lets look at why in spark UI console. There really is not anything we can do to directly influence the cogen or speed it up. It is just there and it works! 
 * When looking at the DAG did we write sloppy SQL / too many stages? So many shuffles that are cluster job is not performing well. Look carefully when there are shuffles see how much
 * data is exchanged between stages. Ex - is the pivot table we had few moments ago. If you run that and look at it on spark youll see there are a bunch of stages. But each stage
 * are small exhanges of data. 
 * 
 * We actually can see a bit more info on the wholeStageCogen. TO do this click the SQL link, this gives us more info about whats been happeneing with the queries.
 * 
 * ID | Desc | Duration | Job IDs
 * 1 | show at Main.java:49 | 10s | [1]
 * 0 | csv at Main.java:36 | 2s | [0]
 * 
 * ID 0 read in the csv file. Recall this is not executed until the first action. That was the root cause of work needing to be done. The more interesting query is ID 1 which is the bulk
 * of the work, took 10 seconds. Drill in here and we can see the wholeStageCodegen box. It had to scan one million rows we are using. Theres a 25.3 s on the top right, this number is
 * the time taken by ALL the tasks added together. Although we are not running on a multi node cluster we are using a multi core processor so there are several threads executing.
 * Next to the 25.3 s we can see the (min time, avg, max) which are (4.2s, 7.0s, 7.1s). If those tasks have run sequentially they would have taken 25.3 seconds.
 * However, we are running in parallel so the duration of the longest one was 7.1 s. Overall for entire job, total time was 10 seconds.
 * 
 * We see something below called HashAggregates. We will come back to this next lecture. WARNING - quite diffiuclt to understand what is going on sometimes when we explore the spark
 * ui. We can get a clue from the spark UI tho.
 */
		
		spark.close();
	
	}
}
