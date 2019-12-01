package com.virtualpairprogrammers.streaming;

import java.util.logging.Level;
import java.util.logging.Level.*;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LoggStreamingAnalys {

	public static void main(String[] args) throws InterruptedException {
		System.getProperty("hadoop.home.dir","c:/hadoop/bin");
	    Logger.getLogger("org.apache.spark.storage").setLevel(Level.SEVERE);	    
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
	    
	    
	    JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));//setting a duration of batching
	    JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost",8989);//our source of streaming data (in this case - localhost)
	    
	   JavaDStream<String> result = inputData.map(item ->item);//result of streaming mapping
	   
	   JavaPairDStream<String, Long> pair = result.mapToPair(rawLogMessage -> new Tuple2<>(rawLogMessage.split(",")[0],1L));
	   pair = pair.reduceByKey((x,y) -> x+y);  
	   
	   
	    pair.print();
	    
	    sc.start();
	    sc.awaitTermination();
	    
	}

}
