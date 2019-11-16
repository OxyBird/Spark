

//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.*;

public class GymCompetitors {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		//Logger.getLogger("org.apache");
		
		//SparkContext context = new SparkContext();
	//	context.setLogLevel("ERROR");
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competetitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		
		//Dataset<Row> csvData = spark.read().option("header", true).csv("src/main/resources/GymCompetitors");
       Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true)
    		   .csv("src\\main\\resources\\GymCompetition.csv");
       
       		   		
	   //csvData.printSchema();
	   //csvData.show();
	   
       StringIndexer genderIndexer = new StringIndexer();
       genderIndexer.setInputCol("Gender");
       genderIndexer.setOutputCol("GenderIndex");
       csvData = genderIndexer.fit(csvData).transform(csvData);
             
       OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
       genderEncoder.setInputCols(new String[] {"GenderIndex"});
       genderEncoder.setOutputCols(new String[] {"GenderVector"});
       csvData = genderEncoder.fit(csvData).transform(csvData);
       csvData.show();
       
       
       
	   
	  VectorAssembler vectorAssembler = new VectorAssembler();
	   vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight", "GenderVector"});
	   vectorAssembler.setOutputCol("features");
	   Dataset<Row> csvDatawithFeatures = vectorAssembler.transform(csvData);
	   
	   csvDatawithFeatures.show();
	   
	   Dataset<Row> modelInputData = csvDatawithFeatures.select("NoOfReps","features").withColumnRenamed("NoOfReps","label");
	   modelInputData.show();
	   
	   LinearRegression linearRegression = new LinearRegression();
	   LinearRegressionModel model = linearRegression.fit(modelInputData);
	   
	   System.out.println("Model has intercept " + model.intercept() + " and coefficient " + model.coefficients());
	   model.transform(modelInputData).show();
	   	      
	}

}
