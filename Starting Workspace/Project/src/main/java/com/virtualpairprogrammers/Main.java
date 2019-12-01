package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.Iterables;


import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		System.getProperty("hadoop.home.dir","C:\\Users\\adm\\Desktop\\Spark studing\\Practicals\\Practicals\\winutils-extra\\hadoop\\bin");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* PairRDD and Pair Key Value
		 * 1 variant Mapping and ReduceByKey for PairRDD, more complicated way
		 * 
		 * JavaRDD<String> originalLogMessages = sc.parallelize(inputData);		
		
		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
			
			String[] columns = rawValue.split(":");
			String level = columns[0];
						
			return new Tuple2<>(level, 1L);
		} );
			
		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
				
		sumsRdd.foreach( tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		sc.close();*/
		
		/*Variant 2 for mapping and reduceByKey pairRDD это и есть LAMBDA - 
		 * возможность писать проще код, начиная с точки
		 */
		 //sc.parallelize(inputData)
		 /*.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":") [0], 1L))
        .reduceByKey((value1, value2) -> value1 + value2)
        .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
        
        sc.close();*/
		
		// FlatMapping
		
		/* JavaRDD<String> sentences = sc.parallelize(inputData);
		
		JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		words.collect().forEach(System.out::println);
		
		sc.close();*/
		 
		 //FILTER using LAMBDa
		 
		 /*sc.parallelize(inputData)
		 .flatMap(vallue -> Arrays.asList(value.split(" ").iterator())
		 .filter(word -> word.length() >1)
		 .collect().forEach(System.out::println);
		 
		 sc.close();*/
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		initialRdd
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.collect().forEach( System.out::println);
		 
		sc.close();
		 
        
		}

}
