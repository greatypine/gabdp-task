package com.gasq.bdp.task;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkWordCount implements GasqSparkTask {

	@Override
	public int run(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("SparkWiordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("E:\\Conference-Track-Management.txt");
		JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> wordOneRdd = wordRdd.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		JavaPairRDD<String, Integer> wordCountRdd = wordOneRdd.reduceByKey((x, y) -> x+y);
		JavaPairRDD<Integer, String> countWordRdd = wordCountRdd.mapToPair(tuple -> new Tuple2(tuple._2, tuple._1));
		JavaPairRDD<Integer, String> sortedCountWordRdd = countWordRdd.sortByKey(false);
		JavaPairRDD<String, Integer> resultRdd = sortedCountWordRdd.mapToPair(tuple -> new Tuple2(tuple._2, tuple._1));
		//resultRdd.saveAsTextFile("E:\\wordCountResult.txt");
		resultRdd.collect().forEach(tuple -> {
			System.out.println(tuple._1 + "," + tuple._2);
		});
		sc.close();
		return 0;
	}
	
	public static void main(String[] args) {
		SparkWordCount swc = new SparkWordCount();
		try {
			swc.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
