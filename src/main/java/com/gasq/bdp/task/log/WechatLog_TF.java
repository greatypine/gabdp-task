/**
 * 
 */
package com.gasq.bdp.task.log;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;
import com.gasq.bdp.task.util.HdfsFileUtil;

/**
 * @author Ju_weigang
 * @时间 2018年8月29日下午2:40:32
 * @项目路径 com.gasq.bdp.task.log
 * @描述 
 */
public class WechatLog_TF  implements GasqSparkTask, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(WechatLog_TF.class);

	public static void main(String[] args) {
		try {
			WechatLog_TF wechatLog = new WechatLog_TF();
			args = new String[2];
//			String inputpath = "hdfs://10.10.20.19:8020/user/wx_log_input/20180905.log";
			String inputpath = "D:\\logs\\20180905.log";
			String output =  "hdfs://10.10.20.19:8020/user/juwg/wechatlog";
			args[0] = inputpath;
			args[1] = output;
			wechatLog.run(args);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		SparkSession spark = getHiveSpark("WechatLog",true);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//		sc.setLogLevel("INFO");
		Configuration hadoopConfiguration = sc.hadoopConfiguration();
		hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[1]);
		JavaRDD<String> textFile = sc.textFile(args[0]);
//		JavaRDD<String> rddwclogs = textFile.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public Iterator<String> call(Iterator<String> t) throws Exception {
//				ArrayList<String> results = new ArrayList<String>();
//				 while (t.hasNext()) {
//					 String f = t.next();
//					 String wxlog = HandleWXlog.handleWXlog(f);
//					 if(StringUtils.isNoneBlank(wxlog)) {
//						 results.add(wxlog);
//					 }
//				 }
//				return results.iterator();
//			}
//        });
		JavaRDD<String> rddwclogs = textFile.map(f->HandleWXlog.handleWXlog(f)).filter(f->f!=null);
//		Dataset<Row> dataFrame = spark.createDataFrame(rddwclogs,String.class);
//		dataFrame.coalesce(1).write().mode(SaveMode.Append).parquet(args[1]);
		rddwclogs.coalesce(1).saveAsTextFile(args[1]);
		logger.info("WechatLog运行完成--------------总用时："+Duration.between(start, Instant.now()).getSeconds()+"秒！");
		sc.close();
		return 0;
	}

}
