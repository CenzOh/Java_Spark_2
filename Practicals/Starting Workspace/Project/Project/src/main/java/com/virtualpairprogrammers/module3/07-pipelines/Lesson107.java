package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lesson107 {

/* Lesson 107 Pipelines
 * 
 * Lets make our code more production standard. We have the model now but lets improve quality. In our first model with the house price prediction we used a lot of spark objects
 * like string indexer, one hot encoder estimator, vector assembler, train validation split, linear regression, helper object. One thing in common is that each of these objects
 * worked in a similar way. Each object we instantiate, set optional properties, call fit or transfer method. Objects we can transform are called transformers, they take a data set
 * as input and return a data set as output for the objects we call fit on, these are called estimators and they take a data set as input and return a transformer.
 * 
 * Estimator (properties) -- fit --> Transformer (properties) -- transform -->
 * 
 * Both transformer and estimator extend abstract class called pipeline stage. Spark lets us combine the pipeline stages into a workflow called pipeline. THis is a different way
 * of building our code and the outcome will be the same. Difference is pipelines are neater. Idea is, using pipelines we create stages and combine them in particular order.
 * DOing that we dont have to keep calling fit or transform for each underlying stage. Spark will do that for us. Neater general structure.
 * 
 * First, we would create the estimators and transformers and set the parameters. Then we create the pipelien object and set up the stages an array of all these estimators
 * and transformers. Call .fit() on pipeline obj to run complete end to end model building process. This runs each estimators and transformers in turn, return obj of type pipeline model.
 * Then we extract the model to look at, or run on test data by calling transform on pipeline model object.
 * 
 * First lets understand about the changes of code we will make. Entire process of running happens at end. Structure is set up pipeline stages, add to pipeline, run pipeline.
 * Currently we have code that does the split of our underlying data into the training, test, and hold up data. It is sensible to do all of that BEFORE building pipeline stages.
 * 
 * To set up the pipeline, go through each object to set them up. Remove the line that does the fit or transform call. Ex is set up indexers but dont run them yet. 
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

		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))) //extra col we calculated manually we may want to use
				.withColumnRenamed("price", "label"); //rename price to label up here so we dont have to worry about it later.
//		csvData.show();
		
		Dataset<Row>[] dataSplits = csvData.randomSplit(new double[] {0.8, 0.2}); //moved this up here was originally below vectorAssembler.
		Dataset<Row> trainingAndTestData = dataSplits[0]; //again this splits to have training and test in one dataset. Then second is the hold out data
		Dataset<Row> holdOutData = dataSplits[1]; 
		
/* ******** DO our indexer BEFORE the vector assembler. Lets do condition first
 */
		StringIndexer conditionIndexer = new StringIndexer();
		conditionIndexer.setInputCol("condition");
		conditionIndexer.setOutputCol("conditionIndex");
//		conditionIndexer.fit(csvData).transform(csvData); //remove all these transform or fit methods to cre
 
//grade indexer
		
		StringIndexer gradeIndexer = new StringIndexer();
		gradeIndexer.setInputCol("grade");
		gradeIndexer.setOutputCol("gradeIndex");
		
//zipcdode indexer
		
		StringIndexer zipcodeIndexer = new StringIndexer();
		zipcodeIndexer.setInputCol("zipcode");
		zipcodeIndexer.setOutputCol("zipcodeIndex");
		

		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"conditionIndex", "gradeIndex", "zipcodeIndex"}); //remember can do this on multi columns
		encoder.setOutputCols(new String[] {"conditionVector", "gradeVector", "zipcodeVector"});
		
//check results
		csvData.show();


		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade", "conditionVector", "gradeVector", "zipcodeVector", "waterfront"} )
				.setOutputCol("features"); //adding our vector fields to input cols also including waterfront didnt need to make waterfront a vector since its already a binary value
	
		VectorAssembler vectorAssembler2 = new VectorAssembler() //THIS IS EXPLAINED LATER IN THE CODE
				.setInputCols(new String[] {"bedrooms", "sqft_living", "sqft_lot", "floors", "grade", "conditionVector", "gradeVector", "zipcodeVector", "waterfront"} )
				.setOutputCol("features"); 
	
		
		LinearRegression linearRegression = new LinearRegression();
		
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

/* We will still build our param map. THis is not a pipeline stage because when we set up the train validation split (which occurs in the pipeline) we have several properties here
 * where we are setting the estimator to be linear regression and estimator param maps to be our param map. Both linear reg obj and map obj MUST exist and to have param map obj, we must build it.
 * We dont run linear regression .fit() so dont worry about removing it since it is being taken care of by train validation split. Dont run train validation split so remove it and 
 * the line to extract the model. We will put it back later on.
 */
		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
				.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
				.build();
		
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit() 
				.setEstimator(linearRegression) 
				.setEvaluator(new RegressionEvaluator().setMetricName("r2")) 
				.setEstimatorParamMaps(paramMap) 
				.setTrainRatio(0.8); 
		
// Lets create pipeline here and instantiate it here. Then call set stages method. Order is important.

		Pipeline pipeline = new Pipeline(); 
		pipeline.setStages(new PipelineStage[] {conditionIndexer, gradeIndexer, zipcodeIndexer, encoder, vectorAssembler, trainValidationSplit}); //import pipelineStage
//		pipeline.setStages(new PipelineStage[] {conditionIndexer, gradeIndexer, zipcodeIndexer, encoder, vectorAssembler2, trainValidationSplit}); 

/* Notice that we included trainValidationSplit but NOT linear regression OR param grid builder. This is because the linear regression model and para map that we built from it are parameters
 * to train validation split. Both can be potential pipeline stage objs, we have used them within this pipeline stage (trainValidationSplit) so they dont get listed as separate stages in the
 * pipeline. Now lets run the pipeline, full end process by calling .fit()
 */
		PipelineModel pipelineModel = pipeline.fit(trainingAndTestData); //returns obj of type pipeline model. Import pipelineModel
		
/* Earlier in code we printed info of underlying model. Lets do that again to see if our model is good or not. Extract the model from the pipelineModel object. The way to do it
 * is a bit complex. But look at this, we created a pipeline by giving the pipeline an array of pipeline stages.
 * 
 * condition Indexer -> grade Indexer -> zipcode Indexer -> encoder -> vector Assembler -> train Validation SPlit
 * 
 * THe pipeline model that will be built has access to each of these stages and the results of each of these stages. Its an array within pipeline model called stages. We need to know
 * which entry in that area will contain the model that we want. It will be the last, train Validation Split. But we need to know which position in the array? Its the 6th item so itll be
 * position 5. To access, look at stage 5 in our pipeline model. 
 */
		
		TrainValidationSplitModel model = (TrainValidationSplitModel)pipelineModel.stages()[5]; 
//access stages with the .stages() method. Returns an array of transformers. Return as TrainValidationSplitModel. The stages returning obj of type transformer so cast to right model.
		
/* Ok we got the underlying model out which was the train validation split model. THis has a method called best model which is what we are interested in. It tests a lot of different
 * parameters to find the best one that gives the best results. Returns a model object so lets cast it to correct model type and store the results. 
 */
		LinearRegressionModel lrModel = (LinearRegressionModel)model.bestModel(); //cast model to correct type and import lr model as well.
		
/* So whats the point of this? Lets say we wanted to do this without one of the features like bathrooms. But we dont know if thats def what we want to do. We would
 * create another vector assembler, remove a particular feature, then swap which of the two stages will be in the pipeline. We can change the vectorAssemberl in pipeline.setStages() and it
 * is very easy to change or remove stages. Lets see what results look like.
 * 
 * training r2 0.82605 RMSE 151652,522668. We received an error in the second part of the output. These results are roughly similar to what we saw before. Error says: 
 * `The field features does not exist`. So this would have occurred when we would try to do the evaluation for the hold out data. On the second sys out line. Holdout data doesnt have
 * features available to us. Why? We created features as part of our vector assembler and vector assembler has only been run on training and test data.
 * SO to be able to evaluate our hold out data, we are going to need to run the first few lines of code against our holdout data. We can do it manually but there is a shortcut:
 * 
 * Once we build the pipeline, use the pipeline to transform the holdout data. Call pipelineModel.transform() which takes in our holdOutData dataset. It runs each of these stages in the
 * pipeline to get the data in correct format. Rather than call fit on last item to generate the model it will call transform on the last item which means it will apply the model to our results.
 * We will get a set of predictions for each of the items in our hold out data.
 */
		
		Dataset<Row> holdOutResults = pipelineModel.transform(holdOutData);
		holdOutResults.show(); //so we can see what it looks like but we still get the error
		
/* ... features        | prediction
 * ... (90,[0,1,2,3... | 456298.52048
 * 
 * We now have a features column with intermediary columns to get to those features, and a set of predictions. SO we can use our pipeline to apply our model to holdout data with a SINGLE line
 * of code. We want to do this evaluation down here but we cant say `we have this data set with the features we need so run evaluation on holdout results`. If you do that we get an error and
 * error says `predictions field already exists. We can get around it inefficiently (efficiently is run steps manually) by taking results and DROP prediction col. After we show them do the drop 
 */
		holdOutResults = holdOutResults.drop("prediction"); //result in no error but removing pred will get r2 and RMSE for holdout data.
		
		System.out.println("Training data r2 value is " + lrModel.summary().r2() + " and the RMSE is " + lrModel.summary().rootMeanSquaredError()); //call lrModel instead of model now 
		
//		model.transform(testData).show(); 
		
//		System.out.println("Testing data r2 value is " + lrModel.evaluate(holdOutResults).r2() + " and the RMSE is " + lrModel.evaluate(holdOutResults).rootMeanSquaredError()); //pred already exists
		
		System.out.println("Testing data r2 value is " + lrModel.evaluate(holdOutData).r2() + " and the RMSE is " + lrModel.evaluate(holdOutData).rootMeanSquaredError()); 
		
		System.out.println("coefficients " + lrModel.coefficients() + " intercept: " + lrModel.intercept());
		System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam());
	}
}