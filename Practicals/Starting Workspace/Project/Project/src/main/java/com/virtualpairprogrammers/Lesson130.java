package com.virtualpairprogrammers;

public class Lesson130 {

/* Lesson 130 Kafka install
 * 
 * Kafka would usually be deployed on a distributed cluster of instances. We want to run locally for development process. On kafka home page we can click on the quickstart guide
 * to get this running locally. You can follow the link to select the apache mirros. Can run on any OS. Issue for windows is all the archives are in .tgz format which
 * is a tar file which ahs been ziped. Windows may not be able to handle .tgz If you cant extract, you can use the 7-zip extraction utility to handle this. Open source software.
 * 
 * You can add this to the C drive. change to kafka directory then bin: C:\kafka_2.11-2.0.0\bin  the scripts are in a directory called windows. Then we can start a ZooKeepr
 * server. THis is another apache software. Common used server, central information store. We have to run this ZooKeeper separately from kafka. Now start
 * the zooKeeper with bin/zookeepr-server-start.sh and point it to a config file called ../../config/zookeeper.properties
 * 
 * Then we should see a lot of logging. Lots of binding support on port 2181. We will use this later. After we see it succeeds, start the kafka server in a new terminal
 * c:/kafka_2.11-2.0.0/bin/windows> kafka-server-start.bat ../../conifg/server.properties
 * 
 * Should run successfully. Step 3, create a topic. A topic in a Public Subscribe System (PSS) is thisL Kafak maintains multiple event ledgers or event logs at same time called a topic.
 * Ex is capture click stream from a website. Topic is logical grouping of events. Then we want to create a topic. Lets keep the topic we want to use. We want to calcualte top 10
 * chart of most watched videos from a video streaming website. Lets call it view records. 
 * 
 * In freesh command prompt, go back to the kafka folder, bin, then windows folder. Command is: kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 
 * --topic viewrecords
 * 
 * May take a few seconds and we should see it created the topic. Finish up step 3 by listing the topics So back in terminal type
 * kafka-topics.bat --list --zookeeper localhost:2181
 * 
 * Should display our viewrecords and any built in topics.
 */
}
