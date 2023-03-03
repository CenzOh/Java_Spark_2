package com.virtualpairprogrammers;

public class Lesson122 {

/* Lesson 122 Intro to Straming
 * 
 * We will look at spark streaming in this module. So far on the course we used spark for batched jobs, we have a set of data prepared and stored somewhere. Feed it in a spark job and outcome
 * the results when done. What we can do is have live data to process in real time at the same time that the data is being generated. THis is where spark streaming comes into play.
 * Some examples - stream of events from a website like clicks, page views, movement of mouse ppointer, etc. Gather all those events, stream them through spark streaming, 
 * do analysis such as live dashboard, info about what users are doing, how many on the website, service log in real time, raise alert if something bad occurs, analyze financial
 * transactions or share price updates as they occur. 
 * 
 * WE can view the spark streaming webpage at https://spark.apache.org/streaming/ Top feature of streaming is its ease of use. We will write streaming jobs in a similar way to how
 * we wrote batch jobs. All of our previous work will transfer easily to the spark streaming API. Spark streaming solves some hard issues like fault tolerance, spark streaming
 * can run for a long time. Practical purpose is to have it run forever. At some point the stream may crash but fault tolerance will be able to recover and jobs can continue. 
 * In this module we will not run on AWS since everything we did from before still applies.
 * 
 * First piece of jargon, D Stream. Note that there are TWO separate versions of streaming / two flavors. In regular spark there is the RDD model and newer spark SQL / data frames / dataset model.
 * Similar story to spark streaming. 
 * 
 * Original ver of spark streaming is called DStreams - traditional model based on RDD API. The newer form of spark streaming is called
 * Structured Streaming - new model based on the Spark SQL engine.
 * 
 * We have access to spark SQL api with this including datasets, sql syntax, etc. 
 * However, if working in spark sql, under the hood we are working with RDDs. Structure streaming is a new model from the ground up. it does NOT contain DStreams inside. There
 * are serious differences between what you can and can not do in the two models. The devs do not plan to do further work on DStreams model. Structured streaming is more elegant, nothing
 * to do with RDDs vs SQL. These streams simply work under the hood a bit different. 
 * 
 * In this course we will still cover some DStreams with basics then move onto structured streaming.
 */
}
