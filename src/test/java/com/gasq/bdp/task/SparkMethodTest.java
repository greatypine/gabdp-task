package com.gasq.bdp.task;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkMethodTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.testing.memory", "2147480000");
		JavaSparkContext sc = new JavaSparkContext("local[*]", "SparkMethodTest", conf);
		
		List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
		JavaRDD<Integer> javaRDD = sc.parallelize(data);
		//转化为pairRDD
		JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {    
		    @Override    
		    public Tuple2<Integer, Integer> call(Integer integer) throws Exception {  
		    	return new Tuple2<Integer, Integer>(integer,1);   
		  }
		});

		JavaPairRDD<Integer,String> combineByKeyRDD = javaPairRDD.combineByKey(new Function<Integer, String>() {    
		    @Override    
		    public String call(Integer v1) throws Exception {  
		      return v1 + " :createCombiner: ";    
		  }
		  }, new Function2<String, Integer, String>() {    
		    @Override    
		    public String call(String v1, Integer v2) throws Exception {        
		      return v1 + " :mergeValue: " + v2;    
		  }
		}, new Function2<String, String, String>() {    
		    @Override    
		    public String call(String v1, String v2) throws Exception {        
		      return v1 + " :mergeCombiners: " + v2;    
		  }
		});
		System.out.println("combineByKey~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(combineByKeyRDD.collect());
		
		JavaPairRDD<Integer,Iterable<Integer>> groupByKeyRDD = javaPairRDD.groupByKey(2);
		System.out.println("groupByKey~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(groupByKeyRDD.collect());

		//自定义partition
		JavaPairRDD<Integer,Iterable<Integer>> groupByKeyRDD3 = javaPairRDD.groupByKey(new Partitioner() {    
		    //partition各数    
		    @Override    
		    public int numPartitions() {        return 10;    }    
		    //partition方式    
		    @Override    
		    public int getPartition(Object o) {          
		      return (o.toString()).hashCode()%numPartitions();    
		  }
		});
		System.out.println("groupByKey Partitioner ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(groupByKeyRDD3.collect());
		
	}

}
