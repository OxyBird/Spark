import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
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

public class CaseStudyML_Logistic {

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
	 

	//for logistic regression here: 1 means customer didn't watch videos, 0 = customer wantched some video
		
		csvData = csvData.withColumn("firstSub",when(col("firstSub").isNull(),0).otherwise(col("firstSub")))
				.withColumn("all_time_views", when(col("all_time_views").isNull(),0).otherwise(col("all_time_views")))
				.withColumn("last_month_views", when(col("last_month_views").isNull(),0).otherwise(col("last_month_views")))
				.withColumn("next_month_views", when(col("next_month_views").$greater(0),0).otherwise(1));	
		
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
		 
		 LogisticRegression logisticRegress = new LogisticRegression();
		 ParamGridBuilder paramGrid = new ParamGridBuilder();
		 
		 ParamMap[] paramMap = paramGrid.addGrid(logisticRegress.regParam(), new double[] {0.01, 0.1, 0.3, 0.5, 1.0})
				 .addGrid(logisticRegress.elasticNetParam(),new double[] {0, 0.5, 1})
				 .build();
		 
		 //6. Give all necessary parameters for trainvalidation: what kind of model, estimator etc
		 
		 TrainValidationSplit trainValidation = new TrainValidationSplit()
		                   .setEstimator(logisticRegress)
		                   .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
		                   .setEstimatorParamMaps(paramMap)
		                   .setTrainRatio(0.9);
		 
		 //7.Apply our model to our datasets (training and holdout datasets)
		 
		 TrainValidationSplitModel model = trainValidation.fit(trainingAndTestingData);
		 LogisticRegressionModel LRModel = (LogisticRegressionModel) model.bestModel(); //we changed a class for model here
		  
		 //8. Reflecting the accuracy of the logistic regression model (one of the main parameter)
		
		 System.out.println("The accuracy of the training/testing model is " + LRModel.summary().accuracy());
		 
		 
		 //9.Reflecting how many correct(positive) prediction we have
		 
		 LogisticRegressionSummary summary = LRModel.evaluate(holdOutData);
		 double truePositive = summary.truePositiveRateByLabel()[1];
		 double falsePositive= summary.falsePositiveRateByLabel()[0];
		 
		 System.out.println("The the houldout data, the likelihood being correct is " + truePositive/(truePositive+falsePositive));
		 System.out.println("The accuracy of the model for holdout data is " + summary.accuracy());
		 
		 //10. Reflecting visual table with correct and incorrect prediction
		 		 
		 LRModel.transform(holdOutData).groupBy("label","prediction").count().show();
		 
		 
	}

}
