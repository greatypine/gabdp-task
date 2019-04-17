package com.gasq.bdp.task;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.gasq.bdp.task.algorithms.FPGrowth4GasqV2;

import scala.Tuple2;


public class SparkFPGrowthTest implements GasqSparkTask, Serializable {
	
	double minSupport = 0.005;
	
    int numPartition = 10;  //数据分区
    double minConfidence = 0.5;//最小置信度
	
	public static void main(String[] args) throws Exception{
		SparkFPGrowthTest task = new SparkFPGrowthTest();
    	task.run(args);
    }

	@Override
	public int run(String[] args) throws Exception {
		
//		double minSupport = 0.005;//最小支持度
//		double minSupport = (double)(2.0/35);
		System.out.println("-----------------------------------------" + minSupport);
        
		SparkSession spark = getHiveSpark("SparkFPGrowthTest",true);	//true 为本地，false为集群。正式环境的设置为集群
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> data = sc.textFile("E://tmp//hello//store29_new.txt");   //minSupport=0.005  list.size=1393
//		JavaRDD<String> data = sc.textFile("E://tmp//hello//store_hlg.txt");  //minSupport=0.1  list.size=35
//		JavaRDD<String> data = sc.textFile("E://tmp//hello//store58.txt");  //minSupport=0.01  list.size=489
//		JavaRDD<String> data = sc.textFile("E://tmp//hello//store_jb.txt");  //minSupport=0.01  list.size=595
//		JavaRDD<String> data = sc.textFile("E://tmp//hello//store310101.txt");  //minSupport=0.1  list.size=140
//		JavaRDD<String> data = sc.textFile("E://tmp//hello//store310112.txt");  //minSupport=0.05  list.size=305
//		Dataset<Row> ds = spark.read().csv("E://tmp//hello//query-hive-35949.csv");
//		ds.show(5);

//		JavaPairRDD<String, List<String>> rs = ds.javaRDD().mapToPair(row -> {
//			String key = row.getString(0);
//			String items = row.getString(1);
//			List<String> arrList =  Arrays.asList(items.split(","));
//			return new Tuple2<String, List<String>>(key, arrList);
//		});
//		rs.groupByKey(numPartition).foreach(new VoidFunction<Tuple2<String,Iterable<List<String>>>>() {
//			
//			@Override
//			public void call(Tuple2<String, Iterable<List<String>>> t) throws Exception {
//				String _1 = t._1;
//				Iterable<List<String>> _2 = t._2;
//				List<List<String>> list = IteratorUtils.toList(_2.iterator());
//				String key = _1+"|"+list.size();
//				System.out.println(key);
//				
//			}
//		});
		
		JavaRDD<List<String>> items = data.map(new Function<String, List<String>>() {

			@Override
			public List<String> call(String line) throws Exception {
				return Arrays.asList(line.split(","));
			}
			
		}).cache();	
	
		List<String> result = FPGrowth4GasqV2.fpgrowth("store00000029",(double)data.count(), minSupport, items);
		result.forEach(System.out::println);
		return 0;
	}
	
	private void fpgrowth(JavaRDD<List<String>> items) {
		FPGrowth fpg = new FPGrowth();
		fpg.setMinSupport(minSupport);
		fpg.setNumPartitions(numPartition);
		FPGrowthModel<String> model = fpg.run(items);
		List<FreqItemset<String>> freqItems =  model.freqItemsets().toJavaRDD().filter(
				fitemset -> (fitemset.javaItems().size() <= 2)).collect();
		System.out.println("/////////////////////////////////////////////////////////");
		freqItems.forEach( fitems -> {
			System.out.println("[" + fitems.javaItems() + "]:" + fitems.freq());
		});
		System.out.println("/////////////////////////////////////////////////////////");
//		model.generateAssociationRules(minConfidence).toJavaRDD().forEach( rule -> {
//			if(rule.javaAntecedent().size() == 1 && rule.javaConsequent().size() == 1) {
//				System.out.println(rule.javaAntecedent() + "-->" + rule.javaConsequent() + ":" + rule.confidence());
//			}
//		});
	}
	
	private double getMinSupportBy(int base, double defaultMinSupport) {
		if(base < 10) {
			return 0.0;
		} else if (base >= 10 && base < 100) {
			return (double)(2.0 / base);
		} else if (base >= 100 && base < 1000) {
			return (double)(4.0 / base);
		} else if (base >= 1000) {
			return defaultMinSupport;
		}
		return defaultMinSupport;
	}

}
