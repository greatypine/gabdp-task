/**
 * 
 */
package com.gasq.bdp.task.algorithms.logisticRegression;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;
import com.gasq.bdp.task.util.BLR_V2;
import com.gasq.bdp.task.util.CommonUtils;
import com.gasq.bdp.task.util.HdfsFileUtil;

/**
 * @author Ju_weigang
 * @时间 2018年8月17日上午10:05:53
 * @项目路径 com.gasq.bdp.task.algorithms.logisticRegression
 * @描述 
 */
public class GALogisticRegression implements GasqSparkTask, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(GALogisticRegression.class);
	public static void main(String[] args) {
		try {
//			args = new String[2];
			String sql = "SELECT a.* from default."+args[0]+" a";
//			String sql = "select * from last_train_data a ";
//			String output =  "hdfs://10.10.20.19:8020/user/juwg/test";
			GALogisticRegression logisticRegression = new GALogisticRegression();
			args[0] = sql;
//			args[1] = output;
			logisticRegression.run(args);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		SparkSession spark = getHiveSpark("GALogisticRegression",false);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.getConf().set("spark.driver.maxResultSize", "8g");
		Configuration hadoopConfiguration = sc.hadoopConfiguration();
        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfiguration.set("dfs.support.append", "true");
        hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[1]);
		/**
		 * Key:除ID外的所有值
		 * value:id
		 */
		Map<String,String> baseKVData = new HashMap<>();
		final Broadcast<Map<String, String>> baseKVbroadcast = sc.broadcast(baseKVData);
		//加载数据，并将数据通过空格分割
		Dataset<Row> dataset = spark.sql(args[0]);
		int len = dataset.columns().length;
		JavaRDD<Row> lines = dataset.toJavaRDD();
		JavaRDD<List<Double>> rsdatardd = lines.map(f->{
			List<Double> tmpList = new ArrayList<Double>();
			int length = f.length();
			for(int i = 1; i < length; i++) {
				Object d = f.get(i);
				tmpList.add(Double.parseDouble(d.toString()));
			}
			Map<String, String> baseKV = baseKVbroadcast.getValue();
			String vstr = StringUtils.join(tmpList, "-").trim();
			String md5 = CommonUtils.md5(vstr);
			baseKV.put(md5,f.getString(0));
			return tmpList;
		});
		List<List<Double>> datas = rsdatardd.collect();
		List<List<Double>> datas1 = CommonUtils.deepCopyList(datas);
		BLR_V2 blr2 = new BLR_V2();
		blr2.DataPreprocess(datas);
		blr2.init(len, 0.8, 1, 0.1, 0.1);
		blr2.GeneralSample(datas, 1);
		blr2.trainModel(datas, 20, "FTRL",10);
		List<String> rsdata = blr2.trainOutResult(baseKVbroadcast.getValue(),datas1, 0.5);
		JavaRDD<String> rsrdddata = sc.parallelize(rsdata);
//		rsdata.forEach(f-> logger.info("输出结果----------------"+f.toString()));
		rsrdddata.coalesce(1).saveAsTextFile(args[1]);
		Instant end = Instant.now();
		logger.warn("GALogisticRegression算法运行完成--------------总用时："+Duration.between(start, end).getSeconds()+"秒！");
		sc.close();
		spark.close();
		return 0;
	}
	
}
