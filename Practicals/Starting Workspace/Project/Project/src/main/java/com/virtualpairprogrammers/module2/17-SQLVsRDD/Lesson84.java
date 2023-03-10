package com.virtualpairprogrammers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import static com.virtualpairprogrammers.SerializableComparator.*;

public class Lesson84 {
	
/* Lesson 84 SparkSQL Performance vs RDDs
 * One last comparison we would like to make is using Spark SQL apis either SPL syntax of java api dataset / dataframe. WE have seen performance of one over the other.
 * We know under the hood, code generated running and transformations are applied to RDDs. Spark SQL is the domain specific language to help us create our RDDs and we can think at
 * higher level of abstraction and think like a data scientist rather than a computer programmer. Instructor has created a new class called LogCountRDDVersion. I will write that code
 * below.
 * 
 * Inside here we implemented the log count using RDDs and make the end results just like what we end up with in Spark SQL version. There is not anything that we have not already done
 * from the earlier section of the course. We have mapToPair(), reduceByKey(), comparator to sortByKey(). To run this file we require a further file called SerializableComparator.java. 
 * This is an interface to allow us to setup a custom sort in Spark.
 * 
 * Does this perform better or worse than Spark SQL? Its not scientific. Doing a test it took the instructor about 23 seconds for the RDD version to run. THis is slightly faster than 
 * Spark SQL. Not massively faster though. Kind of disappointing because if you think about it, under the hood Spark SQL just creates RDDs. If we do the RDDs ourself we should be
 * able to squeeze out faster performance. If we tuned / polished the code we could speed it up faster. Although, local tests on our desktop may not be a reliable benchmark
 * 
 * So lets see the results for running this on a `real cluster` Input 80 million log records about 2 Gbs, 4 workers in the cluster, c5d.xlarge.
 * 
 * SparkSQL 	Spark RDD
 * ~ 2 mins		1.3 mins
 * 
 * These results are interesting. The Spark SQL version about 2 minutes that an average of the SQL syntax and Java API syntax. The Spark RDD version is 1.3 mins so it is ALMOST twice as fast.
 * Again, these are unscientific results. THe point we want to make is the reason for Spark SQL. So if you compare the code for RDD version compared with Java API / SQL version, you can 
 * see the Java and SQL versions take only a few lines while the RDD versions go for many more lines. These include the private methods too with the parsing of months. About 100 lines
 * of code here. SO the point is, if we are doing data mining / experiments against big data sets / spot patterns and trends, Spark SQL is tailor made for this data science.
 * Worry less about code when working with Spark SQL. Working on RDD versions, we really do have to be coders. So RDD version is harder to maintain and understand. SQL is straight forward. 
 * 
 * The great thing about Spark is that we can choose if there is a particular requirement where we have to get low level, then we have RDD at our disposal. If we are concentrating on the data
 * then we can use SQL. Choice is up to us. Instructor recommends dont JUST learn Spark SQL. Learn RDDs as well to understand a bit of what is running under the hood. 
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop"); //same as usual
		Logger.getLogger("org.apache").setLevel(Level.WARN); //same
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark"); //in spark SQL we set sparkSession and use methods like .builder(), .appName(), .master()...
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("Src/main/resources/biglog.txt"); //spark.read().csv()...
		
		// remove the csv header
		input = input.filter(line -> !line.startsWith("level,datetime"));
		
		JavaPairRDD<String, Long> pairs = input.mapToPair(rawValue -> {
			String[] csvFields = rawValue.split(",");
			String level = csvFields[0];
			String date = csvFields[1];
			String month = rawDateToMonth(date); //rawDateToMonth() is a private method defined below
			String key = level + ":" + month; //INFO:January
			return new Tuple2<>(key, 1L); //counting instances of the level on certain month. (INFO:January, 400)
		});
		
		JavaPairRDD<String, Long> resultsRdd = pairs.reduceByKey((value1, value2) -> value1 + value2);
// WARNs are gathered together and ERRORs gathered together. Performs better than grouping. THis crunches the whole RDD down to one value
		
		// order by 
		Comparator<String> comparator = serialize( (a,b) -> {
			String monthA = a.split(":")[1];
			String monthB = b.split(":")[1];
			return monthToMonthnum(monthA) - monthToMonthnum(monthB);
		});
		
		//assuming it is a stable sort, we can sort by secondary first (level) and then sort by primary (month).
		resultsRdd = resultsRdd.sortByKey().sortByKey(comparator);
		
		List<Tuple2<String, Long>> results = resultsRdd.take(100);
		
		System.out.println("Level\tMonth\t\tTotal");
		for (Tuple2<String, Long> nextResult : results) {
			String[] levelMonth = nextResult._1.split(":");
			String level = levelMonth[0];
			String month = levelMonth[1];
			Long total = nextResult._2;
			System.out.println(level+"\t" + month + "\t\t" + total);
		}
		
// SQL reference
//		dataset.createOrReplaceTempView("logging_table"); 
//
//		Dataset<Row> results = spark.sql
//			("select level, date_format(datetime, 'MMMM') as month, count(1) as total, date_format(datetime, 'M') as monthnum "
//			+ "from logging_table group by level, month, monthnum order by monthnum, level");
//		results = results.drop("monthnum"); 
//	
//		results.show(100);
//		
//		results.explain();
	
			
// Java API / DataFrame version		
//		dataset = dataset.select(col("level"), 
//						  date_format(col("datetime"), "MMMM").alias("month"), 
//					  	  date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
//	
//		
//		dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
//		dataset = dataset.drop("monthnum");
// 
//		dataset.show(100);
//		
//		dataset.explain();
		
//		Scanner scanner = new Scanner(System.in); 
//		scanner.hasNextLine();
		
//		spark.close();
	
	}
	
	private static String rawDateToMonth(String raw) { //converting our date to a specific month
		SimpleDateFormat rawFmt = new SimpleDateFormat("yyyy-M-d hh:mm:ss"); //2014-1-15 12:30:13
		SimpleDateFormat requiredFmt = new SimpleDateFormat("MMMM"); //full month name like January
		Date results;
		try {
			results = rawFmt.parse(raw); //convert raw input into our defined format
			String month = requiredFmt.format(results); //from our date, extract the full month name
			return month;
		}
		
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static int monthToMonthnum(String month) {
		SimpleDateFormat rawFmt = new SimpleDateFormat("MMMM");
		SimpleDateFormat requiredFmt = new SimpleDateFormat("M");
		Date results;
		
		try {
			results = rawFmt.parse(month);
			int monthNum = new Integer(requiredFmt.format(results));
			return monthNum;
		}
		
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}
