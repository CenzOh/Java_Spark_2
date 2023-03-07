package com.virtualpairprogrammers;

public class Lesson131 {

/* Lesson 131 Using Kafka Event Simulator
 * 
 * Now our kafka server is setup. We can create our first kafka topic. We called it view records. We will be working for a video streaming platform such as YouTube. Idea is to track
 * which are the most popular videos / most viewings. Not counting just individual viewing. COunt popularity second by second. As a user watches the video, an event will be sent
 * from the video player to the kafka player. We can do it by 5 seconds. Decent level of granularity. Events stream to our servers, capture them.
 * 
 * We have a viewing-figures-generation which can simulate the video streaming platform. Import the proejct into the worksapce, can do it with create new java project,
 * name is viewing-figures-generation, auto configure JRE. The aim of the project  is to simulate the events and send to kafka server. 
 * The pom.xml only has dependency of kafka. Allows us to call kafka from java code. Run the pom.xml as a maven build like usual. Refresh the project. We also have one calss in the project
 * called ViewReportsSimulator.java. Configure the kafka clients with some properties. Where is our kafka server we say its on localhost 9092. 
 * 
 * When we send a message to kafkae we can decide how safe we want the op to be. In acks, when we say all, we want to wait until that message successfully received on all
 * nodes of the kafka cluster. Block until safely copied across cluster. If one of the nodes in the cluster goes down we may lose the message. But if written to main node in cluster
 * we can continue. 
 * 
 * Obj to communicate with kafka is of type producer. Allows us to send messages. We can send a key and value in a pair but we dont need that level of complexity. Just send name of course across kafka
 * Want to say that course has been viewed in the event of the five seconds. When you dont send a key and a value, then you would just send a value. 
 * 
 * Our example file is a long series of numbers and is way too big to open in eclipse. We will split it up with , delimiter, first param is the timestamp and 2nd param is the courseKey.
 * THis is anonymous  so no customer info being given out. Each course has its own number but lets work with meaningful data. We threw a query in ViewReportsSimulator.java to match
 * the course key with course name. Makes it easier to view. 
 * 
 * Next build the report to aggregate the data but we can first test if this works. So in kafka directory, bin and windwos directory, type:
 * kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic viewrecords 
 * 
 * Hit return and we will see our current events that the java program has already made. Youll see a stream of courses on the terminal. CTRL C to terminate job. Kafka is an event log of
 * all of these events being stored in a durable fashion. Go back to the console command and you can see it is picking up current events. Stop the java program.
 * When nothing happens in console, no new events. Cancel again. This time add to it and write --from-beginning so it streams from everything.
 */
}