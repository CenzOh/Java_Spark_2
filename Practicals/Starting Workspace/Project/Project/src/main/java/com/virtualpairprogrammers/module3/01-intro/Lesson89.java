package com.virtualpairprogrammers;

public class Lesson89 {
	
/* Lesson 89 The Model Building Process
 * 
 * We will look at an overview of a model building process. There are several steps we must follow. 
 * First thing to do, choose the right model for the issue we are solving. Do we want linear regression, k means clustering, etc. The model chosen is based on what we are trying to determine
 * and shape our data.
 * Second, decide what data should go in the model. We may have tons of data, not all of it is relevant. Selecting correct data can be time consuming task. Even with spark its
 * a bit of a manual task to get optimal selection of input parameters. To do it well we need a basic understanding of statistics but we will explore some basic variable selection
 * as we progress through the course.
 * Next we need to prepare our data and get it into the right format so we can input it into the model. This is extremely important stage. At the end of the stage we will have features
 * and labels.
 * Next, determine what are called the Model Fitting Parameters. We will look at them later.
 * Next we fit the model meaning we put the data and parameters into Sparks ML process to work out correct intercept and coefficients for the model.
 * Then we evaluate the results. Look at some metrics to determine if the model we have built is going to be accurate enough for our needs. Based on outcome, we may make changes, rerun
 * the model fitting process for a better model.
 * 
 * Its a lot of work and a lot to understand. However, it is not that much code to write. Most of the effort in a real model building process is actually in the data preparation stage.
 * ML means that Spark does a lot of the later steps here for us. We simply just tell Spark what to do.
 * We will be exploring these steps next few chapters but it wont be in order like step 1, step 2. We will instead get a simple model built quick and go back and explore the issues in detail
 * to improve the model over a number of iterations.
 * 
 * Process Recap
 * ------------------------------
 * Choose right model
 * Select input data
 * Data preparation
 * Choose model fitting parameters
 * Fit the model
 * Evaluate
 * 
 * These steps apply to any model we work with. Most of the work is around data manipulation so lets concentrate on just one model type. We will do linear regression.
 * Once we complete this we can do it on another model. Once you do it on one model, you can do it on any model. 
 * 
 * Note, there is a spark ML documentation. On spark documentation website, go to documentation > latest release > programming guides > machine learning. This is a good documentation but
 * can be a bit confusing. Right now, it is possible to write ML with RDDs using Spark SQL. Because of that, the documentation splits into two sections. ML Lib Main Guide uses Spark SQL
 * and the bottom section uses RDD. As of recording, Spark RDD version of Spark SQL has been put in maintenance mode and will be deprecated in the future. Instructor recommends Spark SQL version.
 * Note that its JUSt the RDD version of ML that is being deprecated. RDDs will still be around. Makes sense since Spark SQL is easier to use and using ML is about creating these formulas
 * not really like production job in an environment where performance is critical. So we will use the top section. 
 * In the documentation some areas refer to RDDs which is the older version. We can do this with newer Spark SQL but code will not always look the same. So if you see Java RDDs go back to the top
 * and find the example that uses the SQL API to do it.
 */
	
	
	
}
