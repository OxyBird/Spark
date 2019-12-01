package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Execise2_try {

	public static void main(String[] args) {

		
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		// (chapterId, courseId))
		
		List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
		rawChapterData.add(new Tuple2<>(96,  1));
		rawChapterData.add(new Tuple2<>(97,  1));
		rawChapterData.add(new Tuple2<>(98,  1));
		rawChapterData.add(new Tuple2<>(99,  2));
		rawChapterData.add(new Tuple2<>(100, 3));
		rawChapterData.add(new Tuple2<>(101, 3));
		rawChapterData.add(new Tuple2<>(102, 3));
		rawChapterData.add(new Tuple2<>(103, 3));
		rawChapterData.add(new Tuple2<>(104, 3));
		rawChapterData.add(new Tuple2<>(105, 3));
		rawChapterData.add(new Tuple2<>(106, 3));
		rawChapterData.add(new Tuple2<>(107, 3));
		rawChapterData.add(new Tuple2<>(108, 3));
		rawChapterData.add(new Tuple2<>(109, 3));
		
		
		// Chapter views - (userId, chapterId)
		
		List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
		rawViewData.add(new Tuple2<>(14, 96));
		rawViewData.add(new Tuple2<>(14, 97));
		rawViewData.add(new Tuple2<>(13, 96));
		rawViewData.add(new Tuple2<>(13, 96));
		rawViewData.add(new Tuple2<>(13, 96));
		rawViewData.add(new Tuple2<>(14, 99));
		rawViewData.add(new Tuple2<>(13, 100));
		
		
		// Warm Up count of chapterID in every course (just count all course ID)
		
		JavaRDD<Tuple2<Integer, Integer>> chapterCount =sc.parallelize(rawChapterData);
		
		
		JavaPairRDD<Integer,Integer> countPair = chapterCount.mapToPair(count -> new Tuple2<Integer, Integer> (count._2, 1))
				.reduceByKey((value1, value2) -> value1 + value2);
		
		
				
		countPair.collect().forEach(System.out::println);
		
		
		JavaRDD<Tuple2<Integer, Integer>> userChapters = sc.parallelize(rawViewData);
		JavaRDD<Tuple2<Integer, Integer>> courseChapter = sc.parallelize(rawChapterData);
		
		//Step 1 remove all duplicated views
		
					
		//JavaPairRDD<Integer, Integer> userChaptersPair = userChapters.flatMap(row -> Arrays.asList(row.split(",")).iterator());
		//JavaPairRDD<Tuple2<Integer, Integer>> userChaptersPair = userChapters.flatMap(row -> Arrays.asList(row.split(",")).iterator());
		
		JavaPairRDD<Integer,Integer> pairsUserChapter = JavaPairRDD.fromJavaRDD(userChapters);		
		pairsUserChapter = pairsUserChapter.distinct();				
		pairsUserChapter.collect().forEach(System.out::println);
		
		
		//pairs.sortByKey(true); check it in internet - doesn't work correctly
		
				
		//step 2 get the course ID into RDD
		
		JavaPairRDD<Integer, Integer> pairsCourseChapter = JavaPairRDD.fromJavaRDD(courseChapter);		
		pairsUserChapter= pairsUserChapter.mapToPair(row -> new Tuple2<Integer, Integer> (row._2, row._1));		
		JavaPairRDD<Integer,Tuple2<Integer, Integer>> joinedRdd = pairsUserChapter.join(pairsCourseChapter);
		//мы тут получим Pair RDD из 3х значений: integer + tuple2
		
		// step 3 avoid chapter ID and count how many chapters were watched by users
		
		JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair(row ->{
		Integer UserID = row._2._1;
		Integer CourseID = row._2._2;
		return new Tuple2<Tuple2<Integer, Integer>, Long> (new Tuple2 <Integer, Integer> (UserID, CourseID), 1L);
		});
			
		
		//step 4 count how many users watched course
		
		step3 = step3.reduceByKey((value1, value2) -> value1 + value2);
		
		//step 5 drop userID from PairRDD
		
		JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<Integer, Long> (row._1._2, row._2));
				

		
		// step 6  add total chapters in every course in our PairRDD  (# course, (views and total# of chapters in a course))
		
		 JavaPairRDD<Integer, Tuple2<Long, Integer>> step6= step5.join(countPair);
		 
		 //step 7 convert to percentage values (не трогаем key в нашем Tuple2 из step 6)
		 
		 JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double) value._1/value._2);
		 
		 // step 8 - convert to scores
		 
		 JavaPairRDD<Integer, Long> step8 = step7.mapValues(value ->{
			 
			 if (value >= 0.9) return 10L;
			 if (value >= 0.5) return 4L;
			 if (value >= 0.25) return 2L;
			 return 0L;
			 
			 });
		 
		 //step 9 - total score for each course
		 
		 JavaPairRDD<Integer, Long> step9 = step8.reduceByKey((value1,value2) ->value1 + value2);
		
		 
		step9.collect().forEach(System.out::println);
		

	}

}
