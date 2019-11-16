import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePricesFeatures {

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
		
		//csvData.describe().show();

		csvData = csvData.drop("id","date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");
		
		for (String col:csvData.columns()) {
		
			System.out.println("The correlation between price and " + col + "is " + csvData.stat().corr("price",  col));
								
		}
		
		
	}

}
