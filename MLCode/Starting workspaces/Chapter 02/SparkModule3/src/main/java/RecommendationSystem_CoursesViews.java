import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

public class RecommendationSystem_CoursesViews {

	public static void main(String[] args) {
System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		SparkSession spark = SparkSession.builder()
				.appName("Recommendation System Courses")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		
		Dataset<Row> csvData = spark.read()
    		   .option("header", true)
    		   .option("inferSchema",true) //converts into numerical non-numerical fields (where it's possible)
    		   .csv("src\\main\\resources\\VPPcourseViews.csv");
		
		
		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
		
	//csvData.show();
    //csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show(); //this action took a lot of time for running this program
       		   		
		
		// for recommendation system use ALS model
	
	ALS als = new ALS()
	.setMaxIter(10)
	.setRegParam(0.1)
	.setUserCol("userId")
	.setItemCol("courseId")
	.setRatingCol("proportionWatched");
	
	ALSModel model = als.fit(csvData);
	
	Dataset<Row> userRecom = model.recommendForAllItems(5); //define the number (5) of recommendation for each user 
	List<Row> userRecList = userRecom.takeAsList(5);
	
	for (Row r :userRecList ) {
		int userId = r.getAs(0);
		String recommend = r.getAs(1).toString();
		System.out.println("The user " + userId + "might to be recommended " + recommend);
		System.out.println("This user has already watched ");
		csvData.filter("userId " + userId ).show();
	}
	   		

	}

}
