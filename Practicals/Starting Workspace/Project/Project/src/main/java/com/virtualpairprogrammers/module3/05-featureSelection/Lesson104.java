package com.virtualpairprogrammers;

public class Lesson104 {

/* lesson 104 Data Preparation
 * 
 * We wont do work on our model in the chapter. Lets discuss data prep more generally and formalize the process when building the model.
 * FIrst we aquire our data from somewhere. Extract it from a source system, make it readable format.
 * Second is data cleaning, will talk more in a moment.
 * Third is feature selection, creating variables to include in the model and will look at next chapter.
 * Last, get data in right format for Spark to use. Thats a label and a set of features. 
 * 
 * Lets look more at data cleaning. Determine if data needs work before building model. Our datasets we have used is clean data, good format. Not always the case in real life.
 * Two things to look for:
 * Are there any gaps in the data like missing values? Normal occurrence and would be fine. But Spark does not allow NULL values, we will get an error. So replace the NULLs with either
 * zeros or whichever value we want to use. Think of is there a gap because we dont know the data or is it a mistake like system failure or data loss? If mistake, exclude whole record.
 * Next, think about if there are any errors? Any fields with values we did not expect? Like negative price, date in the future? Then ask ourselves do we exclude the errors?
 * Maybe they are a normal expected outlier? If they dont make sense, exclude them since we will end up with a less meaningful model being produced.
 * 
 * We learned how to do this like filter out values from earlier. All the data we worked with in the course has been cleaned for us. This is normally a job we have to do.
 */
}
