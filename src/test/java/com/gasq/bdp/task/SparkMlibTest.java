package com.gasq.bdp.task;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class SparkMlibTest {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.testing.memory", "2147480000");
		JavaSparkContext sc = new JavaSparkContext("local[*]", "mlib test", conf);
		
		//训练集
		LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(2.0, 3.0, 3.0));
		LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {2, 1, 1}, new double[] {1.0, 1.0, 1.0}));
		
		List<LabeledPoint> l = new LinkedList<>();
		l.add(neg);
		l.add(pos);
		
		JavaRDD<LabeledPoint> training = sc.parallelize(l);
		final NaiveBayesModel nbModel = NaiveBayes.train(training.rdd());
		
		//测试集
		double[] d = {1, 1, 2};
		Vector v = Vectors.dense(d);
		
		System.out.println(nbModel.predict(v));
		System.out.println(nbModel.predictProbabilities(v));
		
	}

}
