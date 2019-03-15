package com.gasq.bdp.task;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.sql.SparkSession;


public class SparkFPGrowthTest implements GasqSparkTask, Serializable {
	
	public static void main(String[] args) throws Exception{
		SparkFPGrowthTest task = new SparkFPGrowthTest();
    	task.run(args);
    }

	@Override
	public int run(String[] args) throws Exception {
		
		double minSupport = 0.006;//最小支持度
        int numPartition = 4;  //数据分区
        double minConfidence = 0.5;//最小置信度
        
		SparkSession spark = getHiveSpark("SparkFPGrowthTest",true);	//true 为本地，false为集群。正式环境的设置为集群
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> data = sc.textFile("E://tmp//hello//store29.txt");
		JavaRDD<List<String>> items = data.map(new Function<String, List<String>>() {

			@Override
			public List<String> call(String line) throws Exception {
				return Arrays.asList(line.split(","));
			}
			
		}).cache();
		
		FPGrowth fpg = new FPGrowth();
		fpg.setMinSupport(minSupport);
		fpg.setNumPartitions(numPartition);
		FPGrowthModel<String> model = fpg.run(items);
		List<FreqItemset<String>> freqItems =  model.freqItemsets().toJavaRDD().collect();
		freqItems.forEach( fitems -> {
			System.out.println(fitems.items().toString() + ":" + fitems.freq());
		});
		
		model.generateAssociationRules(minConfidence).toJavaRDD().collect().forEach( rule -> {
			if(rule.javaAntecedent().size() == 1) {
				System.out.println(rule.javaAntecedent() + "-->" + rule.javaConsequent() + ":" + rule.confidence());
			}
		});
		return 0;
	}

}
