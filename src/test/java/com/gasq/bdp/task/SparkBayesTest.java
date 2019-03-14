package com.gasq.bdp.task;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class SparkBayesTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("NavieBayesTest").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("E://tmp//hello//trainData.txt");
		JavaRDD<LabeledPoint> data = lines.map(new Function<String, LabeledPoint>() {

			@Override
			public LabeledPoint call(String str) throws Exception {
				String[] v = str.split(",");
				String[] ve = v[1].split(" ");
				LabeledPoint a = new LabeledPoint(Double.parseDouble(v[0]),
						Vectors.dense(Double.parseDouble(ve[0]), Double.parseDouble(ve[1]), Double.parseDouble(ve[2])));
				
				return a;
			}
		});
		
//		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.7, 0.3}, 11L);
//		JavaRDD<LabeledPoint> traindata = splits[0];
//		JavaRDD<LabeledPoint> testdata = splits[1];
		
		final NaiveBayesModel model = NaiveBayes.train(data.rdd(), 1.0, "multinomial");
		
		JavaPairRDD<Double, Double> predictionAndLabel = data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

			@Override
			public Tuple2<Double, Double> call(LabeledPoint t) throws Exception {
				return new Tuple2<Double, Double>(model.predict(t.features()), t.label());
			}
		});
		
		//獲取測試精度
//		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double,Double>, Boolean>() {
//
//			@Override
//			public Boolean call(Tuple2<Double, Double> v1) throws Exception {
//				return v1._1.equals(v1._2);
//			}
//		}).count() / (double) testdata.count();
		
		
//		System.out.println("模型精度为：" + accuracy);
		
		//方案一：根据模型计算答案
		JavaRDD<String> testData = sc.textFile("E://tmp//hello//testData.txt");
		JavaPairRDD<String, Double> res = testData.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String t) throws Exception {
				String[] ds = t.split(" ");
				Vector v = Vectors.dense(Double.parseDouble(ds[0]), Double.parseDouble(ds[1]), Double.parseDouble(ds[2]));
				double res = model.predict(v);
				return new Tuple2<String, Double>(t, res);
			}
		});
		
		res.saveAsTextFile("E://tmp//hello//result");
		
		//方案二：保存模型后，导入模型再计算
//		model.save(sc.sc(), "E://tmp//hello//BayesModel");
//		NaiveBayesModel samemodel = NaiveBayesModel.load(sc.sc(), "E://tmp//hello//BayesModel");
		

	}

}
