package com.virtualpairprogrammers;

public class Lesson88 {

/* __________ Lesson 85 welcome to Module 3 __________
 * 
 * __________ Lesson 86 What is Machine Learning? __________
 * 
 * Chapter 1, Intro to Spark ML. In this module we will use spark from machine learning (ML). First lets explain the ideas behind ML and data science. We will use quite complex mathematics
 * and statistical concepts but we dont have to be a good mathematician to take the course. By the end of the course, we will be able to use Spark to create great
 * prediction models and be able to assess how good the models are.
 * 
 * ML is using statistical techniuqes to create models based on our data. SO using ML we should be able to take a set of data and ask spark to create a mathematical formula from it.
 * THis formula will hopefully be a good way to represent our data.
 * 
 * EX: 
 * Age | Height (cm) | Weight (kg) | NoOfReps --> Spark --> (height / 8) + (weight / 4) + (age / 2.5) = NoOfReps
 * 23 | 180 | 88 | 55
 * ...
 * 
 * If this is a good way to rep data, then we can use it to make predictions about future values or inform decisions we make. In our example, we can ask Spark to come up with a formula
 * to predict how many reps an individual may be able to do based on the given input data. Ex - new person is 174 cm, 30 years, 84 kg, we should be able to use the formula to predict
 * how many reps they can do. WIth the formula, it may guess 54 reps. So again, the ML process is coming up with the formula.
 * 
 * ML is not a new concept, instructor says he was helping mathematicians do this kind of work 15 years ago. Then, those people were called statistical model builder / date scientists.
 * Terms still sued today. However, difference for today is that ML automates much of the process of building the model. This makes model building much easier and possible to do for non 
 * mathematicians. There are lots of systems we can use to carry out ML such as Spark.
 * 
 *  Spark has advantages. We can analyze big data sets very easily. We can harness the power of using a network of machines to crunch large amounts of data very efficiently. Sparks
 *  ML libraries are relatively easy to use and powerful. Spark does a lot of the work for us and most of the code is really just getting the data in the right format and
 *  telling Spark to create each of the models. Running the algorithm is very little code
 */
	
/* __________ Lesson 87 Coming up in this module and intro to Kaggle. __________
 * We will look at how to use some of the common ML algorithms in Spark. We will look at 5 in this module. It will be quite easy to swap out one of the models we looked at for any 
 * other type of model that can be useful for us.
 * What we will NOT be doing in the module is an in-depth tutorial on statistics and data science techniques. We simply will lok at getting started but NOT generally looking at the kind
 * of mathematical info. If you want to look at this, check the spark ML documentation. Instructor shows an example of Logistic Regression page. Again, we do not need to be a great
 * mathematician to build great models.
 * 
 * We will use our gym data set example as well as one from Kaggle. This is a website with a huge number of available data sets we can freely use and experiment and learn about ML.
 * Strangely a lot fo the data on this website required manipulation to make them usable. Actually manipulating data is a BIG part of ML. We will cover model building cases
 * since we learned how to manipulate data on the first two modules of the course.
 */
	
/* __________ Lesson 88 Supervised VS Unsupervised learning __________
 * Lets introduce some key terminology. Two general approaches to ML. Supervised learning and Unsupervised learning
 * Supervised learning is when we are trying to predict an outcome, each row within source data has a value we are trying to predict. Ex - gym data we predict how many num of reps each
 * person can do. Our source data contains this answer for some of our competitors and supervised learning allows us to make a formula to be able to predict this number where we dont know it.
 * Another ex, lets say we are a bank deciding who to lend money to. We have lots of data on customers we've lent money to in the past. Because we lent the money in the past, we already
 * know for those customers whether or not they repaid their loans using supervised learning. We can ask spark to help us determine who in the future is also likely to be a good customer
 * and repay their loans and vice versa. Spark would come up with a formula based on the data we have and will use this to make predictions.
 * 
 * Recap - in supervised learning, we have some data to build a model in which we already know the outcome and want to find a way to mathematically calculate a good estimate for the outcome.
 * The outcome we want to predict is called a LABEL. And the variables that might be potential predictors of the label are called FEATURES. Going back to the gym example,
 * num of reps is the label. Each competitors age, height, and weight are the features of our model. We will learn about three different supervised learning models. These are:
 * 
 * Linear Regression which allows us to predict or estimate a value such as the price of a house, num of runs a cricket team might score, or the num of hours of video that a customer
 * might watch. Linear regression is used when we predict a wide range of possible answers.
 * 
 * Logistic Regression allows us to predict if an event will occur or not. Will a loan be repaid or not? Will transaction be fraudulent or good? Logistic Regression used when outcome is
 * a yes or no result.
 * 
 * Decision trees allow us to generate a flowchart to guide decision making process for us. This is used when there are a small or fixed number of possible outcomes. Ex - split customers
 * into groups based on their profitability of high, medium, or low. Decision tree lets us work out the correct group for each customer. 
 * 
 * Alternative to supervised learning i sunsupervised learning. Here, we do not know anything about the outcomes. So we try to find structures and relationships between entities in
 * our data. Ex - we may find that customers in Brazil and Switzerland buy the same types of products but the customers in Venezuela buy very different products. This doesnt seem logical
 * and might not be what we expect. But point is that this could be hidden in the data and unsupervised learning can help find it for us. We will typically look to group the data
 * into segements based on attributes contained within it rather than assumptions and existing knowledge. Ex - if we are selling music and we want to have an option on our
 * website that says you liked Band X you may also like Band Y, an unsupervised model can detect these kinds of connections between bands based on attributes such as avg, tempo, style
 * of music, num of key changes, num of guitar players in the band, etc.
 * 
 * In unsupervised learning we have FEATURES but there is NO label. We will look at two unsupervised models both used as approaches to recommend systems:
 * 
 * K-means allows us to find groups within the data based on its features rather than classifying songs as being pop, folk, classical, etc. K means groups them based on all sorts of
 * attributes so we may not be able to articulate things like tempo and num of different instruments. The outcome will be something like: if you like product X, you may ALSO like product Y
 * as they have similar attribtutes.
 * 
 * Matrix Factorization is another recommender system, outcome will be in the form of `customers who like Product X, also like Product Y`. Its not based on the attributes of the product
 * that we recommend but the attributes of the PEOPLE who are consuming that product. Attributes of our customers.
 * 
 * Whether we use supervised or unsupervised learning, the process of taking our data and using it to build a model is called fitting our model. We would first, pick a model from the list
 * of models available to us such as linear regression (later in course we will understand why to pick one model over another for a particular task). Next we end up with a mathematical formula
 * that has a general form. For linear regression the general form is Y = B0 + B1x1 + B2x2 + ... We generally will not look at these formulas but we will explain this example since its simple.
 * Y is the label value we are trying to predict like in the gym competition the Y represents num of reps. X1, X2, X3 are features / Data points within our data. THats height, weight, age.
 * We want to get to a point where we have a model that says the num of reps a competitor MIGHT be able to do is:
 * 
 * Y = B0 + B1x1 + B2x2 + B3x3 + ...
 * 
 * NoOfReps = 3 + 0.4*height + 0.2*weight + 0.5*age (these numbers are fabricated for simplicity)
 * 
 * Y = NoOfReps
 * X1 = height
 * X2 = weight
 * X3 = age
 * 
 * Fitting the model is about coming up with these values. In the linear regression model, there will be ONE number that we will be adding on every single time. This number is NOT
 * based on the data. In our fabricated data that number is 3. This number is known as the intercept. This is Beta 0 (B0) in our formula. The rest of the values that we are calculating (the other
 * beta values) are known as coefficients of the model. SO the model tries to come up with an intercept (if there is one, not every model has one but there are almost always coefficients). 
 * These are the numbers we will be multiplying by each competitors height, weight, and age to come up with a number of reps.
 * Fitting the model means coming up with the intercept and coefficients so that when we apply these to the features of the model, we can predict the label. 
 */
	
}
