# Udemy Java Spark course:
https://cglearning.udemy.com/course/apache-spark-for-java-developers/learn/lecture/12297260#overview

### Technologies Used:

- Java 1.8

- Eclipse IDE

### Reading the files


### Get Started
First create a folder for this course. I did the following:<br>
`C:\users\cenzo\JavaSparkCourse\`<br>
When you open Eclipse, select the workspace to be the following path: `C:\users\cenzo\JavaSparkCourse\Practicals\Starting Workspace\Project`<br>
In Eclipse, select `Create a new Project` and name it `Project` just like in the file directory. JREs will be auto configured<br>

Make sure we are using Java 1.8. JRE System Library should say jdk1.8.<br>
I had to take the extra step to confi my JDK to be 1.8 by default. When creating the new project, select `Configure JRE`, add new and the path should be: `C:\Program FIles\Java\jre.1.8`<br>
Change compiler level to 1.8 as well, there will be a warning once you swithc the JRE to 1.8.

Next we will check the `pom.xml`, ensure it uses correct dependencies shown in the video. My version should be complete.

Next under the package `src/main/java/com.virtualpairprogrammers` create a new class, Main. Check the create public static void main.

In Main, when we start typing in Spark Conf it will not recognize anything because we have to run our pom.xml file. RIght click > runconfigurations > goasl: eclipse:eclipse. Build should be a success.


### Possible Errors
