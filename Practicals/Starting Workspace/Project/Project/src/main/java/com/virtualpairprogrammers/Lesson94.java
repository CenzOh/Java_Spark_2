package com.virtualpairprogrammers;

public class Lesson94 {

/* Lesson 94 Training vs Test and Holdout Data
 * 
 * Lets go further in our model and evaluate if its good. Recall, last thing we did was NOT meaningful. We looked at predictions by the model for our underlying data. We used one set 
 * of data to build the model and then looked at what the model predicted for the same set of data. Because the model was built using this data, we expect the predictions to be good.
 * But building a model to see if its good at predicting outcomes is mainly for when we dont know the outcomes. Better test would be to see how it performs with DIFFERENT set of data.
 * Data NOT used to fit the model. Normal approach is to split data into two data sets. One is to build model, other is to test model. SO then we can see if the model is good based
 * on how the model performs with data for outcomes that did NOT build the model / that we dont know. Data used to fit the model is known as training data (used to train the model).
 * Data used to assess model for accuracy is test data. Our model only uses 20 rows of data. Not really enough to split into two smaller sets so lets switch over to real world
 * larger data.
 * 
 * We will use a dataset from kaggle website it is called House Sales in King County, USA. Why is predicting house prices good for businesses? Imagine if we work for a bank, and
 * the bank lends money to customers to allow them to buy houses aka mortgages or loans. Bank can have a rule like theyll lend up to 80% of property price. So if a customer
 * wants to buy a property that is 200k, bank is willing to lend up to 160k. So lets say a new customer asks for 100k to buy an apartment for 128k. IE bank lends custoemr 78% of the value
 * of property. Everything here seems fine. But is it? Is that property really worth 128k? If we had a formula to allow us to estimate the price of the property based on its attributes
 * we can see if the price is fair or not. Maybe the customer knows the property is really worth 120k but maybe the seller wants to make it look like its worth more so the customer can
 * borrow enough money. THis could also be an underestimate / property is worth more. Maybe seller would have lowered price to evade tax (but banks point of view this doesnt concern them).
 * 
 * Main point recap - take data about house and their values and rpedict what a house will be worth based on attributes. 
 */
}
