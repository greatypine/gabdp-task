package com.gasq.bdp.task;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import com.gasq.bdp.task.util.CommonUtils;
/**
 * 国安社区Task 用于spark方式运行
 * spark2-submit --master yarn --driver-memory 10g --executor-memory 8g --executor-cores 6 --num-executors 6 --class 
 * @author Renmian
 *
 */
public interface GasqSparkTask extends GasqTask {
	
	public final static String HIVE_WAREHOUSE = "/user/hive/warehouse";
	
	/**
	 * 缺省Spark配置
	 * @param appName
	 * @param isLocal true:为本地测试运行  false:为集群运行
	 * @return
	 */
	default SparkSession getHiveSpark(String appName,Boolean isLocal) {
		if(StringUtils.isBlank(appName)) appName = CommonUtils.createUUID();
		if(isLocal==null) isLocal = true;
		SparkConf conf = new SparkConf().setAppName(appName);
		if(isLocal)conf.setMaster("local[*]");	// 上线需屏蔽
		//resource里定义了hive-site.xml,以下的配置就不需要了
		//spark sql操作hive
		conf.set("spark.sql.warehouse.dir", HIVE_WAREHOUSE);
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		return spark;
	}
	
	/**
	 * 执行run方法
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception;
}
