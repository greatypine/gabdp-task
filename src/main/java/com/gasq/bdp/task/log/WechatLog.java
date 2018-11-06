/**
 * 
 */
package com.gasq.bdp.task.log;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
public class WechatLog  implements GasqSparkTask, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(WechatLog.class);

	public static void main(String[] args) {
		try {
			WechatLog wechatLog = new WechatLog();
//			args = new String[2];
//			String inputpath = "hdfs://10.10.20.19:8020/user/wx_log_input/20180905.log";
//			String inputpath = "D:\\logs\\20180905.log";
//			String output =  "hdfs://10.10.20.19:8020/user/juwg/wechatlog";
//			args[0] = inputpath;
//			args[1] = output;
			wechatLog.run(args);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		SparkSession spark = getHiveSpark("WechatLog",false);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//		sc.setLogLevel("INFO");
		Configuration hadoopConfiguration = sc.hadoopConfiguration();
		hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[1]);
		JavaRDD<String> textFile = sc.textFile(args[0],10);
		JavaRDD<Row> rddwclogs = textFile.map(f->HandleWXlog.handleWXlog1(f)).filter(f->f!=null);
		ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("request_time", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("store_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("customer_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("apptypeplatform", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("task", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("body", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("url", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("x_requested_with", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("user_agent", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("referer", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("mdop", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("adtag", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("source", DataTypes.IntegerType, true);
        fields.add(field);
        field = DataTypes.createStructField("os_family", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("phone_model", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("browser_info", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("network", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("request_date", DataTypes.StringType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(rddwclogs,schema);
		dataFrame.coalesce(1).write().mode(SaveMode.Append).parquet(args[1]);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[0]);
		logger.info("WechatLog运行完成--------------总用时："+Duration.between(start, Instant.now()).getSeconds()+"秒！");
		sc.close();
		return 0;
	}

}
