package com.virtualpairprogrammers;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.applet.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkSQL {
	@SuppressWarnings("resource")
	
	public static void main(String[] args) {
		
		System.getProperty("hadoop.home.dir","c:/hadoop/bin");
	    Logger logger = Logger.getLogger("org.apache");
	    logger.setLevel(Level.WARNING);
	    
	    logger.warning("Start Spark");
		
	
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")//где мы будем актоматически сохранять рещультаты наших SQL запросов)
				                                   .getOrCreate();
		
		
		//Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		
				
		//dataset.show();
		/*long numberOfRows = dataset.count();
		System.out.println("There are " + numberOfRows + " records");
		
		Row firstRow = dataset.first(); 
		String subject = firstRow.getString(2);
		System.out.println(subject); 
		
		String firstRowHeader = firstRow.getAs("subject");
		System.out.println(firstRowHeader);
		
		int year = Integer.parseInt(firstRow.getAs("year"));
		System.out.println("Year is " + year);*/
		
		/* 2. Фильтр, используя SQL подобные запросы
		 * 
		 * Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art' AND year >= 2007");
		modernArt.show();*/
		
		//3. Фильтр, используя lambdas
		
		/*Dataset<Row> modernArtResult = dataset.filter(row -> row.getAs("subject").equals("Modern Art")
													&& Integer.parseInt(row.getAs("year")) >= 2007);
				
		modernArtResult.show();*/
		
		//4. USING SQL QUIRIES
		// создаем temporaly view -  как таблицу, с которой оперируем SQL запросами	
		
		/* dataset.createOrReplaceTempView("my_students_table");	
		Dataset<Row> result = spark.sql("select avg(score) from my_students_table where year = 2007");
		result.show();*/
		
		//5. USING DataFrame, grouping and aggregation
				
		/* List<Row> inMemory = new ArrayList<Row>();
		
		//вставляем данные в массив
		
		inMemory.add(RowFactory.create("WARN","16 December 2018"));
		inMemory.add(RowFactory.create("FATAL","18 October 2017"));
		inMemory.add(RowFactory.create("WARN","21 June 2016"));
		inMemory.add(RowFactory.create("INFO","2 January 2011"));
		inMemory.add(RowFactory.create("FATAL","24 December 2012"));		
		
		//	описываем схему (заголовок таблицы)
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datatime", DataTypes.StringType, false, Metadata.empty())
				};
		StructType schema = new StructType(fields);
		// соединяем данные массива и заголовок
		Dataset<Row> dataset = spark.createDataFrame(inMemory,schema);
		
		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> result = spark.sql("select level, collect_list(datatime) from logging_table group by level");
				
		result.show();*/
		
		// 6. USING big file (1 mln records called biglog.txt) and multiply grouping (когда мы переделываем дата формат из datatemp в конкретный месяц) и считаем их по каждому левелу
		
		
		/* Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");
		
		result.show(100);
		
		// проверка - всего записей должно быть 1 млн. Суммируем total с предыдущего
		
		/*result.createOrReplaceTempView("total_checking");
		Dataset<Row> checking = spark.sql("select sum(total) from total_checking");
		
		checking.show();*/
		
		
		//7. USING Pivot Table (sparl SQL не поддерживает Pivot table - используем Java API для этого
		
		/* Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
			
		dataset = dataset.select(col("level"), date_format(col("datetime"),"MMMM").alias("month"));
		dataset.show();*/
		
		//8. Creating pivot table with Spark (and Java API)
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		dataset = dataset.select(col("level"),
				  date_format(col("datetime"),"MMMM").alias("month"),
				  date_format(col("datetime"), "M").alias("mounthnum").cast(DataTypes.IntegerType));
		
		// here we used method pivot as a relational Grouped Dataset (т.е. группируем по 2м колонкам level (идет в строки) - month(в колонки)
		//Используем pivot с параметрами колонка и list<Object> для того, чтобы по горизонтали отображались месяцы как в нашем созданном array, а не по алфавиту
		
		Object[] months = new Object[]{"January","February","March", "April","May", "June","July","August","September","October","November","December"};
		List<Object> columns = Arrays.asList(months);
		
		
		dataset = dataset.groupBy("level").pivot("month", columns).count();
			
		dataset.show(100); 
						
			
				
		spark.close();
		

	}

}
