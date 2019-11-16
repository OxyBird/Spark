import static org.apache.spark.sql.functions.col;

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

public class HosePricesAnalysis_Pipelines {

	public static void main(String[] args) {
System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		SparkSession spark = SparkSession.builder()
				.appName("HousePrice Analysis")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		
		Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true)
    		   .csv("src\\main\\resources\\kc_house_data.csv");
             		   		
	   
		csvData = csvData.withColumn("sqft_above_percentage",col("sqft_above").divide(col("sqft_living")))
				         .withColumnRenamed("price", "label");
		
				
		 Dataset<Row>[] dataSet = csvData.randomSplit(new double[] {0.8,0.2});
	     Dataset<Row> trainingAndTestData = dataSet[0];
	     Dataset<Row> holdOutData = dataSet[1];
		
		
		//Reverse non-numeric field "condition" into numeric vector and add it into the model
		 
		StringIndexer conditionIndex = new StringIndexer();
		conditionIndex.setInputCol("condition");
		conditionIndex.setOutputCol("conditionIndex");
		
		
		StringIndexer zipcodeIndex = new StringIndexer();
		zipcodeIndex.setInputCol("zipcode");
		zipcodeIndex.setOutputCol("zipcodeIndex");
				
		StringIndexer gradeIndex = new StringIndexer();
		gradeIndex.setInputCol("grade");
		gradeIndex.setOutputCol("gradeIndex");
			
			
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"conditionIndex","zipcodeIndex","gradeIndex"});
		encoder.setOutputCols(new String[] {"conditionVector","zipcodeVector","gradeVector"});
		
       
       VectorAssembler vectorAssembler = new VectorAssembler()
                       .setInputCols(new String[] {"bedrooms", "bathrooms","sqft_living","sqft_above_percentage","floors","conditionVector","zipcodeVector","gradeVector","waterfront"})                       
                       .setOutputCol("features");
       
            
       LinearRegression linearRegression = new LinearRegression();
       ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
       
       ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(),new double[] {0.01,0.1,0.5})
    		                                  .addGrid(linearRegression.elasticNetParam(),new double[]{0,0.5,1})
    		                                  .build();
       
       TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
    		         .setEstimator(linearRegression)
    		         .setEvaluator(new RegressionEvaluator().setMetricName("r2")) 
    		         .setEstimatorParamMaps(paramMap)
    		         .setTrainRatio(0.8);
              
       
       Pipeline pipeline = new Pipeline();
       pipeline.setStages(new PipelineStage[] {conditionIndex, zipcodeIndex, gradeIndex, encoder,vectorAssembler,trainValidationSplit});
       PipelineModel pipelineModel = pipeline.fit(trainingAndTestData);
       TrainValidationSplitModel model = (TrainValidationSplitModel)pipelineModel.stages()[5];
       LinearRegressionModel lrmodel = (LinearRegressionModel)model.bestModel();
       
       Dataset<Row> holdOutDataResults = pipelineModel.transform(holdOutData);
       holdOutDataResults.show();
       
       holdOutDataResults = holdOutDataResults.drop("prediction");
       
       System.out.println("The r2 of training data model is " + lrmodel.summary().r2() + " and RMSE for trainig data model is " + lrmodel.summary().rootMeanSquaredError());
                   
       System.out.println("The r2 of testing data model is " + lrmodel.evaluate(holdOutDataResults).r2() + " and RMSE for trainig data model is " + lrmodel.evaluate(holdOutDataResults).rootMeanSquaredError());
		
	}

	}

