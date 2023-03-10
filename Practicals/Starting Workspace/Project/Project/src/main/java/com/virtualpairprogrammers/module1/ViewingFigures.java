//Finally looking at in Lesson 38
//package main.java.com.virtualpairprogrammers;
package com.virtualpairprogrammers;



import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyze viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true; //can put to false after lesson 47 to test it on big data
	/* OUTPUT for big data of the three views csv:
	 * (5964, Spring Boot Microservices)
	 * (5940, Spring Remoting and Webservices)
	 * 
	 * (2044, Java Fundamentals)
	 * (716, HTML5)
	 */
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!
		//Warmup
		JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair( row -> new Tuple2<Integer, Integer>(row._2, 1))
				.reduceByKey( (value1, value2) -> value1 + value2);
		chapterCountRdd.foreach(System.out::println);
		
/* First get our chapter RDD> DO a mapping. We will call it a row even though its not a DB table. First value is going to be what was fed in but the second part of what was fed in
 * (courseID), then for the second value it will be an integer of 1. Continuing on the one line / fluently, we will do reduce by key, takes any two values and crunch them down by adding the two
 * values together. This gives us our resulting RDD which is of type integer, integer. TO test this, just do a quick print line using for each. All pretty routine.
 * Note that we ARE in test mode so all results should come out to agree with what we have on the captions. Note that the ordering may be different. Output is good.
 */
		
	//Step 1 - remove any duplicated views!
		viewData = viewData.distinct(); //no need to make a new variable, we can reassign it
		viewData.foreach(System.out::println);; //sanity check, make sure user 13 watching chapter 96 occurs ONCE.
//part of lesson 39. Using distinct method to remove any form of duplicates
		
	//Step 2 - get course Ids into the RDD Lesson 40
		viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1)); //flip transformation;
		viewData.foreach(System.out::println); //sanity check
		
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData); //join to original chapter Data not the one with the counts
		joinedRdd.foreach(System.out::println); //sanity check
		
	//Step 3 - dont need chapterIds, set up for a reduce (hardest step! Because we have to deal with Tuple2s) Lesson 41
		JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair( row -> {
			Integer userId = row._2._1; //lets set these explicitly, easier to understand ._2 to get the tuple, ._1 to get userID
			Integer courseId = row._2._2; //same idea, ._2 to get tuple ._2 to get courseID
			return new Tuple2<Tuple2<Integer, Integer>, Long>( new Tuple2<Integer, Integer>(userId, courseId), 1L); //((14,1),1). ((userID, courseId), count).
		});
		step3.foreach(System.out::println); //sanity check
		
	//Step 4 - count how many views for each user per course. should be easier now! Lesson 42
		step3 = step3.reduceByKey((value1, value2) -> value1 + value2); //reduce method to count the views. Remember views is a long
		step3.foreach(System.out::println);; //should get a count of the views.
		
	//Step 5 - remove the user ids Lesson 43
		JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2 )); //int is the course, long is the count/views
		//first value, ._1 to get the Tuple, ._2 to get course ID. Second value, ._2 to get the views
		step5.foreach(System.out::println); //(3,1). (courseId, views)
		
	//Step 6 - add in the total chapter count Lesson 44
		JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRdd);
		step6.foreach(System.out::println); //(1,(1,3)). (courseId, (views, of))
		
	//Step 7 - convert to percentage
		JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double)value._1 / value._2); //casting to double allows us to do floating point operation instead of int division
		step7.foreach(System.out::println); //(3, 0.1) (courseId, percent)
		
	//Step 8 - convert to scores
		JavaPairRDD<Integer, Long> step8 = step7.mapValues( value -> {
			if (value > 0.9) return 10L;
			if (value > 0.5) return 4L; //dont need to do < 0.9 because lambda would return the value if it was greater than 0.9
			if (value > 0.25) return 2L;
			return 0L;
		});
		step8.foreach(System.out::println); //(2,10) (courseId, score)
		
	//Step 9 - add up total scores
		step8 = step8.reduceByKey((value1, value2) -> value1 + value2);
		step8.foreach(System.out::println); //(1,6) (courseId, totalScore);
		
	//Step 10 - join with titles rdd
		JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step8.join(titlesData);
		step10.foreach(System.out::println); //(2,(10, Work faster harder smarter until you drop)). (courseId, (totalScore, title))
		
	//Step 11 - sort by score
		JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<Long, String>(row._2._1, row._2._2));
		//Long for the totalScore. String for the Title. key, ._2 gets the Tuple, ._1 gets the totalscore. value, ._2 gets Tuple, ._2 gets title.
		step11.sortByKey(false).collect().forEach(System.out::println); //false for it to sort by ascending. (10, Work faster...) (score, title).
		
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}