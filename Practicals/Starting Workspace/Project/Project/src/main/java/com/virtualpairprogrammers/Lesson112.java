package com.virtualpairprogrammers;

public class Lesson112 {

/* Lesson 112 True / False Negatives and Positives
 * 
 * So far we learned how to use linear regression model with Spark. In fact we have seen entire syntax and process flow of spark ML algorithms. We can swap linear regression with any other
 * model type and use exact same syntax and structure. Every model in spark is built from Transformers and Estimators. We have to pick the right transformers and estimators properties
 * Correctly for the model type we want to use. Our second model type is logistical regression, similar to linear reg. Difference is linear reg predicts a numerical outcome / wide range
 * of numbers available as result of outcome. Last chapter ex was predicting num of videos a customer may watch from 0 - very high number.
 * 
 * Log reg is predicting a boolean value such as is a customer a good customer or not. Fraud or real transaction, spam or not spam email examples. Label we will input will either be 
 * 0 or 1. Outcomes will be between a 0 or 1. Probability of events occurring.  Ex - outcome is 0.5, we can say its true. If its less than 0.5, false. Inputs to the model is same as
 * linear regression. Just remember labels are 0 or 1. Features are the same. Model fitting parameters also the same. Reg param parameter and elastic net parameter.
 * 
 * Accuracy is different. log reg d/n have r2 and root mean squared error. Our measure of accuracy is called accuracy. This is the % of correct predictions. Ex - if our input was 100 entries
 * and model predicted 91 percent of them correctly, our accuracy will be 0.91. Closer accuracy is to 1, the better. Misclassification is the opposite of accuracy. Proportion of
 * predictions that are incorrect. Sometimes we want more than just accuracy and misclassification rate. Ex - using a model to find out if a patient in a hospital has a particular illness.
 * Again we are trying to predict if a patient is suffering from an illness given a num of features about the patient. Knowing we can predict with outcome of 95% alone doesnt mean
 * that 95% likely someone has that illness.
 * 
 * Lets discuss why. WE have ten entries in our dataset. 1 means individual has the illness. In this example 8 correct predictions so 80% accuracy or 0.8. We need to dig deeper tho
 * For each prediction, we want to know where it predicted a 1, did they really have an illness? Label every entry as:
 *  true positive (expected a one and got a one)
 *  true negative (expected a 0 and got a 0)
 *  false positive (expected a one and got a 0)
 *  false negative (expected a 0 and got a 1)
 *  
 *  The accuracy score would be the sum of all the true values, the true positive and true negative. These are the correct predictions. One way we can then measure the accuracy is:
 *  Look at how many positives were there, both when correct prediction and incorrect. True positive + false positive = 7. Now ask, how many were really correct?
 *  True positive = 6. False positive = 1. Use these numbers to divide the true positives by the total positives to get the likelihood that if the customer tests positive, they have the illness.
 *  
 *  If positive, then likelihood that really is positive = 6/7 = 0.85. This is higher than our accuracy score of 0.8. However, it could have been lower. It has to do with the combinations of
 *  true and false positives and negatives! We can consider this the other way around as well. If we test the customer and their test is negative, likelihood that it really is negative
 *  is 2/3 = 0.67. Again we get these numbers from: True negative = 2. True negative + false negative = 3.
 */
}
