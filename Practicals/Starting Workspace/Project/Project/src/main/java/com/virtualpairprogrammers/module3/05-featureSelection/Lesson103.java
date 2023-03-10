package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col; //importing column function

public class Lesson103 {
	
/* Lesson 103 Identifying and Eliminating Duplicated Features
 * 
 * The next issue we have to address is duplication within variables. Ex - is sqft_living and sqft_living15 the same thing? Instrucotr thinks living is the size of property and living
 * 15 is the size of property in 2015 but its not clear in the description. Maybe owners added extension and increased space so there would be a slightly different size.
 * Again not clear if thats the case. Lets just check if these two fields are related to each other since their correlation is strong and in the right direction. We want to pick
 * one of these fields but not both. Which field do we pick? THe one with the higher correlation figure. So lets pick sqft_living and DROP sqft_living15.
 * 
 * Now to our feature selection list we need to ask if there are any duplicate variables that contain the same data as each other or very strongly correlated data with each other.
 * General rule is to AVOID having this in our model as it will lead to issues later on if we have variables that are strongly related to each other.
 * The way we can find this out is by doing a double loop. Consider the correlation of each variable with each other variable.
 */

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadooop"); 
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header",  true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv"); 

//		csvData.describe().show();
		
		csvData = csvData.drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode", "lat", "long"); 
		
		for (String col : csvData.columns()) { 
			System.out.println("Correlation between price and " + col + " is " + csvData.stat().corr("price", col) );
		}
		
		csvData = csvData.drop("sqft_lot", "sqft_lot15", "yr_built", "sqft_living15"); //removing cols that have small correlation 
		
// this time we will do a double loop
		for(String col1 : csvData.columns()) {
			for (String col2 : csvData.columns()) {
				System.out.println("Correlation between " + col1 + " and " + col2 + " is " + csvData.stat().corr(col1, col2) ); //removed price and added in col1 and col2
			}
		}
		
/* A lot of data was printed I cant type it all so I will write some of the highlights. First off, we would normally take all this data and put it into a table. This is known as
 * a correlation matrix.
 * 			| Price | Bedrooms | Bathrooms
 * Price	|   1	|   0.308  |  0.525
 * Bedrooms | 0.308 |     1	   |  0.515
 * Bathrooms| 0.515 |   0.525  |   1
 *...
 *
 * Remember that correlation works in both directions, for instance sqft_basement and price is the same as price and sqft_basement. Both are 0.323. So putting it in a table is 
 * easy to read. We can ignore the diagonal since they are comparing with one another. One that is highly correlated is sqft_above and sqft_living at 0.876. These two fields are ALMOST
 * the same. sqft_living is probably the total of a sqft_above and sqft_basement. Looks like a three way relationship. We only want one of these three fields.
 * Pick the field based on which has higher correlation. sqft_living and price has a correlation of 0.702 while sqft_above and price has a correlation of 0.605 so we want to pick
 * sqft_living.
 * 
 * Note we must use our best judgment. Bathrooms and sqft_living have a high correlation with each other with 0.755. We may think to only use sqft_living and ignore everything else
 * and that is worth trying. But its safe to say the number of bedrooms will be useful same with num of bathrooms. So its tough theres no fast rule. We want to exclude thing that
 * have a correlation of things that are really close to 1 or -1 here.
 * 
 * Back to the three way relationship issue of sqft_living, sqft_above, sqft_basement. if we can calculate the third field from the other two, the rule is to only include one or two of these
 * fields never include all three. But theres another alternative, calculate something new from the data. Maybe sqft_living and a portion of the sq footage is above ground and not
 * part of the basement which may be a sensible metric to include. Maybe calculate a new value and use that.
 * 
 * Let us adjust our model now. We will build a model with the bedrooms, bathrooms, sqft_living, and floors and a new field which is percentage of sqft above ground and lose the rest of the variables
 * Now we would hop back to the house price analysis.java file but I will include everything about what to do below:
 */
//adding our new col, calculate by doing sqftabove divide by sqft living. Import col with static
		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))); 
		csvData.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage", "floors" } ) //adding sqft above percentage field
				.setOutputCol("features");
		
		Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
				.select("price", "features")
				.withColumnRenamed("price",  "label");
		
/* We can see our new column was added to the end of our csv file
 * sqft_above_percentage
 * 1.0
 * 0.84435
 * ...
 * 
 * Looks pretty good. Properties with a 1 mean they have no basement. Second one says about 85% of property is above ground / not basement. Lets check r2 and RMSE
 * r2 0.5104 and RMSE 255680.14855. 
 * r2 0.4931 and RMSE 266176.20405.
 * Looks like we unfortunately managed to make the r2 go down and make a worse model. We may be able to improve this by adding in some fields back that are non numeric
 */
		
		
	}
}
