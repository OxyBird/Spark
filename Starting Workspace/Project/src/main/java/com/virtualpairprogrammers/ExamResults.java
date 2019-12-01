package com.virtualpairprogrammers;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResults {

	public static void main(String[] args) {

		System.getProperty("hadoop.home.dir","c:/hadoop/bin");
		Logger.getLogger("org.apache").setLevel(Level.WARNING);
			
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")//где мы будем актоматически сохранять рещультаты наших SQL запросов)
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		//Using agg (agregation) method. Otherwise just max, min etc methods get into trouble(for example, different datatypes etc)
		// также groupby метод ВСЕГДА идет в аггрегейшн функцией (count, average, min, max, etc). Agg метод позволяет
		//- использовать несколькоагрегирующий функций за раз через запятую и позволяет НЕ МЕНЯТЬ тип данных в колонке(например, string to integer)
		//обязательно тут используем метод functions и импортируем библиотеку под него. Чтобы можно было манипулировать колонками
		
		/*dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score"),
				                                 min(col("score")).alias("min score"));*/
		
		
		
		/*Creation pivot table with different aggregations functions like average, standard deviation etc
		 * 
		 * dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score")),2).alias("average score"),
				                                               round(stddev(col("score")),2).alias("standard deviation"));*/
		
		/* if we want to use boolean and depending on grade result (A+ - D) figure out does student pass, use UDF (User Defined Function - 
		 * is a feature of Spark SQL to define new Column-based functions that extend the vocabulary of Spark SQL’s DSL for transforming Datasets) 
		 * method and lambda */
		
		spark.udf().register("hasPassed",(String grade, String subject) -> {
			
			if (subject.equals("Biology")) 
			{
				if (grade.startsWith("A")) return true;
				return false;
				}
						
		return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}, DataTypes.BooleanType);

		dataset = dataset.withColumn("Pass",callUDF("hasPassed",col("grade"), col("subject")));                                        
											             
		
		dataset.show();
		
		spark.close();
		
			}

}
