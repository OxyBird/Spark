import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors_unsuperClasteringML {

	public static void main(String[] args) {
System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competetitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		
		
       Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true)
    		   .csv("src\\main\\resources\\GymCompetition.csv");
       
       		   		
	   //1. Encoding our male-female categorical field
       
       StringIndexer genderIndexer = new StringIndexer();
       genderIndexer.setInputCol("Gender");
       genderIndexer.setOutputCol("GenderIndex");
       csvData = genderIndexer.fit(csvData).transform(csvData);
             
       OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
       genderEncoder.setInputCols(new String[] {"GenderIndex"});
       genderEncoder.setOutputCols(new String[] {"GenderVector"});
       csvData = genderEncoder.fit(csvData).transform(csvData);
       csvData.show();
       //2. Created vector assembler - features for our model
       VectorAssembler vectorAssembler = new VectorAssembler();
       Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"GenderVector", "Age", "Height", "Weight", "NoOfReps"})
                       .setOutputCol("features")
                       .transform(csvData).select("features");
       
       //3. Created cluster model 
       KMeans kMeans = new KMeans();
       
       //trying to figire out the optimal number of cluster - created a loop
       
       for (int noOfCluster = 2; noOfCluster<=8; noOfCluster++) {          
       
       kMeans.setK(noOfCluster);
       System.out.println("The number of clusters is " + noOfCluster);	          
       
       KMeansModel model = kMeans.fit(inputData);
       Dataset<Row> predictions = model.transform(inputData);
       predictions.show();
       
       //4.Reflecting each cluster center (average for each feature) (this code's block is not using in our loop for num of clusters)
       
       //Vector[] clusterCenter = model.clusterCenters();
       //for (Vector v : clusterCenter) {System.out.println(v);}       
             
       predictions.groupBy("prediction").count().show(); //counting how many features are in every cluster 
       
       //5. Looking at accuracy of the model by calculating next models parameters:
       
       System.out.println("The SSE is " + model.computeCost(inputData));       
       ClusteringEvaluator evaluator = new ClusteringEvaluator();
       System.out.println("The silhouette with square euclidian dictance " + evaluator.evaluate(predictions));
       }
       
	}

}
