package com.virtualpairprogrammers;
import java.util.List;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Array;
import scala.Tuple2;

public class Execise {

	@SuppressWarnings("resource")
	
	public static void main(String[] args) {
	System.getProperty("hadoop.home.dir","C:\\Users\\adm\\Desktop\\Spark studing\\Practicals\\Practicals\\winutils-extra\\hadoop\\bin");
	Logger.getLogger("org.apache").setLevel(Level.WARNING);
	
	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<String> initialRdd = sc.textFile("C:\\Users\\adm\\Desktop\\Spark studing\\Practicals\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\subtitles\\input.txt");
	JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
	
	JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0);
	JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
	
	JavaRDD<String> blankWordsRemoved = justWords.filter (word -> word.trim().length() > 0);
	JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));
	
	JavaPairRDD<String,Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<String,Long> (word, 1L));
	
	JavaPairRDD<String,Long> totals = pairRDD.reduceByKey((value1,value2)-> value1 + value2);
	JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));
		
	JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
	
	List<Tuple2<Long,String>> result = sorted.take(50);
	
	result.forEach(System.out::println);
	
	Scanner scanner = new Scanner(System.in);
	scanner.nextLine();	
	
	sc.close();

	}

}
