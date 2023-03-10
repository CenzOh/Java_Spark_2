package com.virtualpairprogrammers;

public class Lesson90 {
	
/* Lesson 90 Intro Linear Regression
 * 
 * We will learn how to build our first model which is Linear Regression. Lets deep dive into what it can be used for. WARN - code in this chapter is NOT going to be complete
 * and NOT good quality model YET. We will start slow and build up our knowledge. To recap we have seen general form of the linear regression model:
 * 
 * Y = B0 + B1X1 + B2X2 + B3X3 + ...
 * NoOfReps = 3 + 0.4*height + 0.2*weight + 0.5*age
 * 
 * Y - label like NumOfReps
 * B0 - intercept like 3
 * B1, B2, B3 - coefficients such as 0.4, 0.2, 0.5
 * X1, X2, X3 - features like height weight age
 * 
 * We want to produce this formula to predict a label based on using an intercept to calculate a num and then a series of coefficients for each of the features of our model.
 * We want to come up with the formula like the example above. Linear Regression can predict a continuous numerical value. This is the label. The value we want to predict will be
 * a number with an infinite range of possible values. Ex - predict price of house, probability customer pays back loan. Can use linear regression on these examples. 
 * We had the simple example of the predicting number of reps based on height, age, weight. 
 * We will continue to use the local gym example. 20 competitors took place. If we know age, height, weight and gender, can we predict how many reps they can do?
 * 
 * Lets explore the data a bit more. A common thing data scientists would do is to create a scatterplot of the variables against the outcome. We will not be doing this in Spark but
 * we can go through the process to see.
 * In our example we created three graphs. Y axis is num of reps. X axis (diff for each graph) is age, height, and weight. We ignored gender for now. 
 * In the age graph, we can see competitors whoa re 23 (there are four of them) range is between 45 to 55. And looking at the height graph also seems like the values are a bit random.
 * No obv connection between competitors age and num of reps as well as their height and num of reps. Hmm strange.
 * 
 * However, looking at the weight graph we can see a correlation / relationship. You can draw a line through the graph to see how as the weight increases, the more num of reps 
 * a competitor may be able to do. The line represents a formula. THe formula would be: NoOfReps = 23 + 0.37 * weight. This will give us the estimate. This formula is the red
 * line on the graph. So if a competitiors weight was 75 kgs, they may be able to do 52 reps. This is an estimate. Also this is a valid linear regression model.
 * We have a formula to predict where we have intercept of 23 and one feature of weight. Calculation was made with excel. Not a perfect approximation. Things not that simple. 
 * Especially since competitors num of reps is calculated with combo of their height, weight, age, and gender. Other factors not listed as well.
 * 
 * Lets come up with a BETTER formula to use more factors. This will be hard since we need to add more than one feature. Not easy way to do it on graphs so lets do it and put it in tables
 * instead. 
 */

}
