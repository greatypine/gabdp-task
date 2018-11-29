package com.gasq.bdp.task.logn;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.gasq.bdp.task.util.DateUtil;
import com.gasq.bdp.task.util.HdfsFileUtil;

/**
 * 
 * @author Renmian
 * log demo
 * date time remoteClientIp storeId customerId appPlatform {task_info}	mac
 * 2018-07-09 00:00:00 192.168.0.33 8ad8cc8f635d6c580163625807874ff9 69fa9aff99ca44b785662fcd6927bd4f customer_android {"method":"appSev","client":"1","app_com":"com_appcart","com":"com_appService","task":"newAppList","requestTimestamp":"2018-07-09 00:00:00"} null
2018-07-09 00:00:01 192.168.3.7 8ad8e18b5b9c5cc2015bf17074e261ee bfade7a21d3747a68ea36dd2feaa0b0d customer_ios {"client":1,"requestTimestamp":"2018-07-09 00:00:01","method":"appSev","app_com":"com_appshop","com":"com_appService","task":"getStoreTel"} null
2018-07-09 00:00:02 10.48.250.213 8ad8958c60f78f5b0160fcb19b2a2fa3 2f37b368a46a483db1dab37d9ecbcef2 customer_ios {"page":1,"method":"appSev","requestTimestamp":"2018-07-09 00:00:02","app_com":"com_appshop","com":"com_appService","row":18,"tags":"天津开荒保洁","task":"tagprolist","client":1} null
2018-07-09 00:00:04 192.168.1.100 8ad8ac865b4441e4015b5b03630f4d83 7abe81da09894ce49b73134bf771c0f8 customer_android {"app_com":"com_appyp","page":"1","com":"com_appService","task":"eshopprolist","method":"appSev","row":"18","client":"1","category_id":"9c70658d1dce3e378b44e3174dffcbd4","eshop_id":"6e5b8be2bf7dfd4e494d36a59cea712b","requestTimestamp":"2018-07-09 00:00:04"} null
2018-07-09 00:00:06 192.168.0.33 8ad8cc8f635d6c580163625807874ff9 69fa9aff99ca44b785662fcd6927bd4f customer_android {"com":"com_appService","app_com":"com_appcart","method":"appSev","task":"total","client":"1","requestTimestamp":"2018-07-09 00:00:06"} null
2018-07-09 00:00:08 10.48.250.213 8ad8958c60f78f5b0160fcb19b2a2fa3 2f37b368a46a483db1dab37d9ecbcef2 customer_ios {"client":1,"requestTimestamp":"2018-07-09 00:00:08","method":"appSev","id":"8f2bcacce6afee6b63ef2e66dfdd13f5","app_com":"com_appshop","com":"com_appService","width":414,"task":"proinfo","pro_code":""} null
2018-07-09 00:00:09 192.168.1.100 8ad8ac865b4441e4015b5b03630f4d83 7abe81da09894ce49b73134bf771c0f8 customer_android {"app_com":"com_appyp","page":"1","com":"com_appService","task":"eshopprolist","method":"appSev","row":"18","client":"1","category_id":"0309b344659ff3bd2a5c953d3ad8f203","eshop_id":"6e5b8be2bf7dfd4e494d36a59cea712b","requestTimestamp":"2018-07-09 00:00:09"} null
 */
public class GuoanshequEdianLogParser implements LogParser {
	
	private static final Logger logger = LoggerFactory.getLogger(GuoanshequEdianLogParser.class);
	public static final String EDIAN_LOG_DELIMITER = "\\s";	//空格分隔符

	/**
	 * 参数1：input
	 * 参数2：output
	 * 参数3：相对今天的数据往前推
	 * 参数4：日志表分隔符，默认“/t”
	 * 输出格式：GuoanshequEdianLogParser inputPath outputPath dateDiff TERMINATED
	 * eg:GuoanshequEdianLogParser /user/es_input /user/log_output/t_log -1
	 * or(full):GuoanshequEdianLogParser /user/es_input /user/log_output/t_log -1 \t
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 3) {
			logger.error("参数1：input");
			logger.error("参数2：output");
			logger.error("参数3：相对今天的数据往前推");
			logger.error("参数4：日志表分隔符，默认“/t”");
			logger.error("输出格式：GuoanshequEdianLogParser inputPath outputPath dateDiff TERMINATED");
			logger.error("参数1,2,3必须输入！");
			System.exit(-1);
		}
//		String inputPath = "hdfs://master:8020/user/es_input/20180909.log";
//		String outputPath = "hdfs://master:8020/user/renmian/test";
		GuoanshequEdianLogParser parser = new GuoanshequEdianLogParser();
		parser.run(args);
	}
	
	/**
	 * 按行解析获取GuoanshequEdianLog对象
	 * 解析错误返回空对象
	 * @param line
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Row parseLine(String line, String TERMINATED) {
		try{
			GuoanshequEdianLog log = new GuoanshequEdianLog(TERMINATED);
			int start = line.indexOf("{");
			int end = line.lastIndexOf("}")+1;
			if(start > 0 && start < end) {	//解析json字符串
				String json = line.substring(start, end);
				JSONObject obj = JSONObject.parseObject(json);
				log.setApp_com(obj.getString("app_com"));
				log.setTask(obj.getString("task"));
				obj.remove("requestTimestamp");
				obj.remove("method");
				obj.remove("app_com");
				obj.remove("com");
				obj.remove("task");
				obj.remove("client");
				log.setBody(JSONObject.toJSONString(obj));
				line = line.substring(0, start) + line.substring(end+1);	//替换掉{json}字符串以及中间的一个空格符
			}
			String[] strArr = line.split(EDIAN_LOG_DELIMITER);
			if(!isValidLogFormatter(strArr)) {		//无效日志
//				System.err.println("error line match:::" + line);
				logger.info("error line match:::" + line);
				return null;
			}
			log.setRequest_date(strArr[0]);
			log.setRequest_time(strArr[1]);
			log.setIp(strArr[2]);
			log.setStore_id(strArr[3]);
			log.setCustomer_id(strArr[4]);
			log.setApptypeplatform(strArr[5]);
			log.setMac_address(strArr[6]);
			return RowFactory.create(log.getColumns());
		} catch (Exception e) {
			logger.error(e.getMessage() + ":::" + line);
		}
		return null;
	}
	
	/**
	 * 做基本日志格式值的校验
	 * 7个字段+有效日期+有效时间格式
	 * 以后格式有要求在这里扩展
	 * @param msgArr
	 * @return
	 */
	private static boolean isValidLogFormatter(String[] strArr) {
		//strArr 国安社区扩展字段在最后一列 @2018-09-28
		return strArr.length >= 7 && DateUtil.isValidDate(strArr[0]) && DateUtil.isValidTime(strArr[1]);
	}
	
	
	@Test
	public void doTestMatchJSON() {
		//String testStr = "2018-07-09 00:00:00 192.168.0.33 8ad8cc8f635d6c580163625807874ff9 69fa9aff99ca44b785662fcd6927bd4f customer_android {\"method\":\"appSev\",\"client\":\"1\",\"app_com\":\"com_appcart\",\"com\":\"com_appService\",\"task\":\"newAppList\",\"requestTimestamp\":\"2018-07-09 00:00:00\"} null";
//		String testStr = "2018-07-09 00:09:59 100.23.136.48 8ad8a0885a44fb3d015a64cc1ee643ff null customer_android {\"method\":\"save\",\"com\":\"com_appService\",\"task\":\"userActSta\",\"client\":\"1\",\"app_com\":\"com_user\",\"data\":[{\"num\":13,\"booth\":\"8ad88884635d6c8201635e273b1911d5\",\"cell\":\"8ad8828b6366a325016366c146b203ea\",\"exh\":\"homepage5.18\",\"jc\":\"93c622fc05d95b30c57e7d10d156d294,e929328afe20d2edbb2b1b85746e1707,5491f96bdcbfc5695adf5c5f98f3f6a6,23202c3212ebc0b30201af602deded0b,ebfa7201446708daf6abb5aa0d822bf8,92a1b72bb8093693565cb5df15653b5d,7de1a8a6e3329a154ce501f5e7eb5ec0\",\"jt\":\"sku\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 09:48:01\"},{\"num\":14,\"booth\":\"8ad88884635d6c8201635e273b1911d5\",\"cell\":\"8ad8828b6366a325016366c146b203ea\",\"exh\":\"homepage5.18\",\"jc\":\"93c622fc05d95b30c57e7d10d156d294,e929328afe20d2edbb2b1b85746e1707,5491f96bdcbfc5695adf5c5f98f3f6a6,23202c3212ebc0b30201af602deded0b,ebfa7201446708daf6abb5aa0d822bf8,92a1b72bb8093693565cb5df15653b5d,7de1a8a6e3329a154ce501f5e7eb5ec0\",\"jt\":\"sku\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 09:49:07\"},{\"num\":15,\"booth\":\"8ad88884635d6c8201635e273b1911d5\",\"cell\":\"8ad8828b6366a325016366c146b203ea\",\"exh\":\"homepage5.18\",\"jc\":\"93c622fc05d95b30c57e7d10d156d294,e929328afe20d2edbb2b1b85746e1707,5491f96bdcbfc5695adf5c5f98f3f6a6,23202c3212ebc0b30201af602deded0b,ebfa7201446708daf6abb5aa0d822bf8,92a1b72bb8093693565cb5df15653b5d,7de1a8a6e3329a154ce501f5e7eb5ec0\",\"jt\":\"sku\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 09:49:18\"},{\"num\":16,\"booth\":\"8ad88884635d6c8201635e273b1911d5\",\"cell\":\"8ad8828b6366a325016366c146b203ea\",\"exh\":\"homepage5.18\",\"jc\":\"93c622fc05d95b30c57e7d10d156d294,e929328afe20d2edbb2b1b85746e1707,5491f96bdcbfc5695adf5c5f98f3f6a6,23202c3212ebc0b30201af602deded0b,ebfa7201446708daf6abb5aa0d822bf8,92a1b72bb8093693565cb5df15653b5d,7de1a8a6e3329a154ce501f5e7eb5ec0\",\"jt\":\"sku\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 09:49:24\"},{\"num\":17,\"booth\":\"8ad88884635d6c8201635e273b1911d5\",\"cell\":\"8ad8828b6366a325016366c146b203ea\",\"exh\":\"homepage5.18\",\"jc\":\"93c622fc05d95b30c57e7d10d156d294,e929328afe20d2edbb2b1b85746e1707,5491f96bdcbfc5695adf5c5f98f3f6a6,23202c3212ebc0b30201af602deded0b,ebfa7201446708daf6abb5aa0d822bf8,92a1b72bb8093693565cb5df15653b5d,7de1a8a6e3329a154ce501f5e7eb5ec0\",\"jt\":\"sku\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 09:49:37\"},{\"num\":18,\"booth\":\"8ad8fa83635c372901635d3dce591bee\",\"cell\":\"8ad8cc8363f19ecf0163f71556503d51\",\"exh\":\"homepage5.18\",\"jc\":\"jws\",\"jt\":\"exhibition\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 10:09:44\"},{\"num\":19,\"booth\":\"8ad8d092630b5bc001634e0028fd7ef9\",\"cell\":\"8ad8cc8f635d6c5801635d73583700b5\",\"exh\":\"jws\",\"jc\":\",保姆,月嫂,\",\"jt\":\"group\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 10:09:48\"},{\"num\":20,\"booth\":\"8ad8d092630b5bc001634e0028fd7ef9\",\"cell\":\"8ad8cc8f635d6c5801635d73583700b5\",\"exh\":\"jws\",\"jc\":\",保姆,月嫂,\",\"jt\":\"group\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 10:13:53\"},{\"num\":21,\"booth\":\"8ad88289630b5bbb01634e07e8dd7eec\",\"cell\":\"8ad88884635d6c8201635d760f6200ce\",\"exh\":\"jws\",\"jc\":\",保姆,\",\"jt\":\"group\",\"phone\":\"13595162685\",\"time\":\"2018-06-30 10:14:32\"}],\"requestTimestamp\":\"2018-07-09 00:09:59\"} 00:90:4c:c5:26:d9";
		String testStr = " null  null   null";
		String[] arr = testStr.split(EDIAN_LOG_DELIMITER);
		System.out.println(arr.length);
		
		
//		GuoanshequEdianLog log = parseLine(testStr);
//		System.out.println(log.toString());
//		int start = testStr.indexOf("{");
//		int end = testStr.lastIndexOf("}");
//		System.out.println(testStr.substring(start, end+1));
	}

	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		
		//格式化日志时间  yyyyMMdd.log
		String inputPath = args[0]+"/"+ DateUtil.getDiyStrDateTime(Integer.parseInt(args[2]),DateUtil.DATE_NO_FLAG_DATE_FORMAT) +".log";	//输入参数
		String outputPath = args[1];
		//for test
//		inputPath = "hdfs://master:8020/user/es_input/20180909.log";
//		outputPath = "hdfs://master:8020/user/renmian/test";
		
		Configuration hdfsConf = getConf();
		hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
		SparkSession spark = getHiveSpark("Guoanshequ.Edian.LogParser",false);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("warn");
		HdfsFileUtil.removeHfile(hdfsConf, outputPath);
		JavaRDD<String> lines = sc.textFile(inputPath);
//		JavaRDD<GuoanshequEdianLog> logs = lines.map(new Function<String, GuoanshequEdianLog>() {
//
//			@Override
//			public GuoanshequEdianLog call(String line) throws Exception {
//				// TODO Auto-generated method stub
//				return parseLine(line);
//			}
//		}).filter(new Function<GuoanshequEdianLog, Boolean>() {
//			@Override
//			public Boolean call(GuoanshequEdianLog log) throws Exception {
//				return log != null;
//			}
//		});	
		String TERMINATED = (args.length > 4 && StringUtils.isNotBlank(args[3])) ? args[3] : "\t";	//与sqoop表导入语句中的TERMINATED定义的参数保持一致
		//lambda写法
		//清洗和处理日志
		@SuppressWarnings("resource")
		JavaRDD<Row> logs = ((JavaRDD<Row>)lines.map(line -> {
													GuoanshequEdianLogParser parser = new GuoanshequEdianLogParser();
													return parser.parseLine(line, TERMINATED); })).filter(log -> log != null);
		//parquet 文件列映射	
		ArrayList<StructField> fields = new ArrayList<StructField>();
		StructField field = DataTypes.createStructField("request_time", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("ip", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("store_id", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("customer_id", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("apptypeplatform", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("app_com", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("task", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("body", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("mac_address", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("request_date", DataTypes.StringType, true);
		fields.add(field);
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(logs,schema);
		dataFrame.coalesce(1).write().mode(SaveMode.Append).parquet(outputPath);
		//RDD写文件输出
		//logs.coalesce(1).saveAsTextFile(outputPath);
		
		GuoanshequEdianLog log = new GuoanshequEdianLog(TERMINATED);
//		spark.sql(log.getCreateOuterTblHql(hiveWarehousePath));	//执行外部表创建---failed!!!
		spark.sql(log.loadDataToTbl(outputPath, DateUtil.getDiyStrDateTime(Integer.parseInt(args[2]),DateUtil.DEFAULT_DATE_FORMATTER)));	//load数据到外部表
		sc.close();
		Instant end = Instant.now();
		logger.info("parse log@[" + inputPath + "] cost:[" + Duration.between(start, end).toMillis() + "]ms");
		return 0;
	}

}
