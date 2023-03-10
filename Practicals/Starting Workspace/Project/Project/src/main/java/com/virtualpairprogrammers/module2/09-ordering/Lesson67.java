package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Lesson67 {
	
/* Lesson 67 Ordering
 * Last time, we made a report of all the ERRORs, FATALs, etc from the big log txt. Its all output in a random order so lets fix that. One option is to add the order by to the main SQL
 * as seen before. After doing group can do order by.
 */
	
	@SuppressWarnings("resource") 
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
	
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
				
		Dataset<Row> big_dataset = spark.read().option("header", true).csv("Src/main/resources/bigLog.txt");
		
		big_dataset.createOrReplaceTempView("logging_table");	
		
		Dataset<Row> bad_results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month order by month"); //one option

/* The problem with this: Results does not have natural order, its alpha order by month. April, August, December, etc
 * How to handle? Many ways to do this. One thing is to add a column which is numerical month starting with 1 run to 12. Order by that column instead.
 */
		Dataset<Row> bad_results1 = spark.sql
				("select level, date_format(datetime, 'MMMM') as month, date_format(datetime, 'M') as monthnum, count(1) as total "
						+ "from logging_table group by level, month order by monthnum"); //added month num col so we can have numerical version.
		
/* THERE WAS A CRASH. Why was that? Error message says `datetime is neither present in group by nor is it an aggregate function...` 
 * There is a fault adding the additional column, we are grouping by level and month so there are two rows where the level and month are identical. What about monthnum? 
 * If it did execute we would get results like this: 
 * 
 * Level | Month | Count(1) | Monthnum
 * WARN | December | 2 | [12, 12]
 * 
 * We dont want this^^ so we must group by the monthnum as well. If we dont care which value we get (which we dont) just wrap the month num with first() function
 */

		Dataset<Row> bad_results2 = spark.sql
				("select level, date_format(datetime, 'MMMM') as month, first(date_format(datetime, 'M')) as monthnum, count(1) as total "
						+ "from logging_table group by level, month order by monthnum"); //added first() fcn for month num
		
/* It prints and in weird order January, October, November, February, etc. Does an alphabetical sort so 1 is first, 10 is next, 11, then 12, then 2. This occurred because date_format method
 * returns a string so spark sql assumes to sort by string. Deal with this by casting one type to another for month num, we have done this before so let us use it explicitly now. 
 */
		Dataset<Row> bad_results3 = spark.sql
				("select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total "
						+ "from logging_table group by level, month order by monthnum"); //casting month num as an integer
	
/* Output is looking a bit better. HOWEVER we dont really want to see the month num column in final output. We can get rid of it by dropping it in java api.
 * drop method lets us pass in a string, pick col name. Recall this is immutable again, will not modify so it will return a new dataset.
 */
		bad_results3 = bad_results3.drop("monthnum");
		
/* There is an easier way we can do this. The report still takes same amount of time with that extra column or not. We actually did not need to use the java api. If want to use sql only
 * then there is an optimization. We create the month num col to use in order by clause. It is fine in spark SQL to call a function in the order by! Instead of the col lets put it in the order by
 */
		Dataset<Row> bad_results4 = spark.sql
				("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
						+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int)"); //direct call to date fcn in order by, no extra col
		
/* Output works like we expect. THe report is easy to read. Can pick out some nice patterns. WE can still improve it, we will look at that in a future chapter on pivot tables.
 * Now when you look at lets say December, you can see the numerical values are in a random order. WARN 8328, DEBUG 41749, FATAL 94. And in November we have ERROR 3389, INFO 23301, DEBUG 33366.
 *  Different order. Lets be consistent with secondary order by.
 */
		Dataset<Row> results = spark.sql
				("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
						+ "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"); //adding sort by level as WELL. 12 sub sorts on the levels
		
/* Output shows logs in alpha order: DEBUG, ERROR, FATAL, INFO, WARN. All in order now. Much better. Next time, looking at how to do this in Java API.
 */
		
		results.show(100); 
		
		spark.close();
	
	}
}
