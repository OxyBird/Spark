import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class DecisionTree_Trial {
	//1. Creat a interface with categorizing our countries for 6 groups
	
	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

		@Override
		public String call(String country) throws Exception {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});
			
			if (topCountries.contains(country)) return country; 
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}
	};
	
	public static void main(String[] args) {
System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		
		SparkSession spark = SparkSession.builder()
				.appName("Marketing Analysis")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		//2. We define udf (spark SQL) - user define function ( define function as an input parameter of udf function. 
		// It accepts Scala (Java) functions of up to 10 input parameters. 
		
		spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);
				
		Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true)
    		   .csv("src\\main\\resources\\vppFreeTrials.csv");
       
		//3. calling UDF function - changed our column "country" according to UDF
		//create "label" column with two values: 0 and 1 (all that bigger than 0 is equal 1)
		
		csvData = csvData.withColumn("country", callUDF("countryGrouping", col("country")))
				          .withColumn("label",when(col("payments_made").geq(1),lit(1)).otherwise(lit(0)));
       
		//4. Convert categorical column (string) into numerical (index)
		
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country")
		            .setOutputCol("CountryIndex")
		            .fit(csvData).transform(csvData);
		// Reflect indexes with their values(countries) using class IndexToString
			
		new IndexToString()
		    .setInputCol("CountryIndex")
		    .setOutputCol("value")
		    .transform(csvData.select("CountryIndex")).distinct()
		    .show();
	
		
		csvData.show();
		//5. Create a vector assembler for features 
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"CountryIndex", "rebill_period", "chapter_access_count", "seconds_watched"})
		               .setOutputCol("features");
		              
		
		Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label","features");
		// inputData.show();
		      
		//6. Devided our dataset in two patrs: training and testing datasets
		Dataset<Row>[] trainingAndholdOutData = inputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = trainingAndholdOutData[0];
		Dataset<Row> holdOutData = trainingAndholdOutData[1];
		
		//7. Created model decision tree classifier
		
		DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier();
		dtClassifier.setMaxDepth(3); // 3 is the number of branches in the tree
		
		DecisionTreeClassificationModel model = dtClassifier.fit(trainingData);
		Dataset<Row> predictions = model.transform(holdOutData);
		predictions.show();
		
		System.out.println(model.toDebugString());//toDebugString method shows us the model results (subtotals, 
		// how it works under the hood) as a string on the console
		
		//8. Checking the accuracy of this model.
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		System.out.println("The accuracy of the decision tree model is " + evaluator.evaluate(predictions));
		
		
		//9. Using random tree forest model (trying to raise up our accuracy of prediction.
		
		RandomForestClassifier RFclassifier = new RandomForestClassifier();
		RFclassifier.setMaxDepth(5); // 5 is the number of branches in the each tree. We can increase an accuracy by playing this number. 
		
		RandomForestClassificationModel ForestModel = RFclassifier.fit(trainingData);
		Dataset<Row> predictions2 = ForestModel.transform(holdOutData);
		predictions2.show();
		
		System.out.println(ForestModel.toDebugString()); //toDebugString shows us the model in strings
	
		System.out.println("The accuracy of the random tree forest model is " + evaluator.evaluate(predictions2));
		
		
		
		
		
		
		
		
		
		
		
			
		
		
	  
	}

}
