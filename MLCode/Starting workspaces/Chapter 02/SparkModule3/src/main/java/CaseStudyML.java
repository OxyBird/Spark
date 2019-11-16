import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.spark.ml.*;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class CaseStudyML {

	public static void main(String[] args) {
	
System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		
		SparkSession spark = SparkSession.builder()
				.appName("Marketing Analysis")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		
		Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true)
    		   .csv("src\\main\\resources\\CaseStudyML\\*.csv");
       
       		   		
	   //csvData.printSchema();
	   //csvData.show();
		
		
	//1. Filtering dataset (take out gone customers) + for other some columns replace null = 0
		
		csvData = csvData.filter("is_cancelled = false").drop("is_cancelled","observation_date");
		
				
		csvData = csvData.withColumn("firstSub",when(col("firstSub").isNull(),0).otherwise(col("firstSub")))
				.withColumn("all_time_views", when(col("all_time_views").isNull(),0).otherwise(col("all_time_views")))
				.withColumn("last_month_views", when(col("last_month_views").isNull(),0).otherwise(col("last_month_views")))
				.withColumn("next_month_views", when(col("next_month_views").isNull(),0).otherwise(col("next_month_views")));	
		
		csvData = csvData.withColumnRenamed("next_month_views", "label");
		
		//2. Encoding categorical columns (not numerical), making them vectors in our dataset
		
		StringIndexer payMethodIndex = new StringIndexer();
		csvData = payMethodIndex.setInputCol("payment_method_type")
		              .setOutputCol("payMethodIndex")
		              .fit(csvData)
		              .transform(csvData);
		
		StringIndexer countryIndex = new StringIndexer();
		csvData = countryIndex.setInputCol("country")
		            .setOutputCol("countryIndex")
		            .fit(csvData)
		            .transform(csvData);
		
		StringIndexer periodIndex = new StringIndexer();
		csvData = periodIndex.setInputCol("rebill_period_in_months")
		            .setOutputCol("periodIndex")
		            .fit(csvData)
		            .transform(csvData);
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		encoder.setInputCols(new String[] {"payMethodIndex", "countryIndex", "periodIndex"});
		encoder.setOutputCols(new String[] {"payMethodVector", "countryVector", "periodVector"});
		csvData = encoder.fit(csvData).transform(csvData);	
		
		
		
		//3. Create vectors (features) and label in our dataset for our model 
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		
		Dataset<Row> inputData = vectorAssembler
				 .setInputCols(new String[] {"payMethodVector","countryVector","periodVector", 
						 "firstSub", "age", "last_month_views"})
				 .setOutputCol("features")
				 .transform(csvData)
				 .select("label","features");
		 
		inputData.show();
			
		 //4. Dividing our dataset for testing and holdout data
		 
	     Dataset<Row>[] splitDataSet = inputData.randomSplit(new double[]{0.9 , 0.1});
		 Dataset<Row> trainingAndTestingData = splitDataSet[0];
		 Dataset<Row> holdOutData = splitDataSet[1];
		 
		 //5. Setting regular parameters and create a grid for testing model. This numbers in arrays are random from my head
		 
		 LinearRegression linearRegress = new LinearRegression();
		 ParamGridBuilder paramGrid = new ParamGridBuilder();
		 
		 ParamMap[] paramMap = paramGrid.addGrid(linearRegress.regParam(), new double[] {0.01, 0.1, 0.3, 0.5, 1.0})
				 .addGrid(linearRegress.elasticNetParam(),new double[] {0, 0.5, 1})
				 .build();
		 
		 //6. Give all necessary parameters for trainvalidation: what kind of model, estimator etc
		 
		 TrainValidationSplit trainValidation = new TrainValidationSplit()
		                   .setEstimator(linearRegress)
		                   .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
		                   .setEstimatorParamMaps(paramMap)
		                   .setTrainRatio(0.9);
		 
		 //7.Apply our model to our datasets (training and holdout datasets)
		 
		 TrainValidationSplitModel model = trainValidation.fit(trainingAndTestingData);
		 LinearRegressionModel LRModel = (LinearRegressionModel) model.bestModel(); //we changed a class for model here
		 
		
		 System.out.println("The r2 of the training/testing model is " + LRModel.summary().r2() + " and RMSE is " + LRModel.summary().rootMeanSquaredError());
		 
		 System.out.println("The r2 of the holdout model is " + LRModel.evaluate(holdOutData).r2() + " and RMSE is" + LRModel.evaluate(holdOutData).rootMeanSquaredError());
		              
		 
		 		 
		 
		 
		 
		 
		 
		 
				  
				
				
				
				
				
				
				
	   
		
		

	}

}
