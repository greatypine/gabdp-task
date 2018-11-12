package com.gasq.bdp.task.log;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;
import com.gasq.bdp.task.util.ImpalaUtils;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
  * Description: es 读取搜索日志数据
  * @author lyy  
  * @date 2018年11月8日
 */
public class SearchLog implements GasqSparkTask, Serializable {

	
	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(SearchLog.class);
	private static final String tableName = "t_search_log";
	
	public static void main(String[] args) {
		try {
			SearchLog searchLog = new SearchLog();
//			String[] arr = new String[2];
//			arr[0] = "C:\\Users\\Administrator\\Desktop\\logs\\2018-11-08.log";// 读路径
			searchLog.run(args);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		//重建表还是insert表
		boolean isCreate = true;
		//输入两个参数,create  或者  insert 默认create
		if(args.length >1){
			String param = args[1];
			if(param!= null && !"".equals(param)){
				if("insert".equals(param.toLowerCase())){
					isCreate = false;
				}
			}
		}
		
		SparkSession spark = getHiveSpark("SearchLog",false);
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 //商品id和商品名称的map
		ImpalaUtils ipu = new ImpalaUtils();
        String sql = "select id , content_name from gabdp_user.view_product_id_name ";
        Map<Object, Object> product_id_name_map = ipu.getMap(sql);
        final Broadcast<Map<Object, Object>> broadcast_product_id_name_map = jsc.broadcast(product_id_name_map);
        JavaRDD<String> textFileRDD = jsc.textFile(args[0],10);
        
        SimpleDateFormat dateSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final Broadcast<SimpleDateFormat> broadcast_sdf = jsc.broadcast(dateSdf);
        //封装产生结果集
		 JavaRDD<Log_Search> resultRDD = textFileRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Log_Search>() {

			private static final long serialVersionUID = 1570154238203739202L;

			@Override
			public Iterator<Log_Search> call(Iterator<String> iterator) throws Exception {
				SimpleDateFormat sdf = broadcast_sdf.value();
				Map<Object, Object> productMap = broadcast_product_id_name_map.value();
				
				List<Log_Search> resultList = new ArrayList<Log_Search>();
				
				while(iterator.hasNext()){
					String line = iterator.next();
					String[] arr = line.split("-\\{");
					
					if(arr.length >1){
						Log_Search logS = new Log_Search();
						
						String jsonStr= "{"+arr[1];
						try{
							JSONObject fromObject = JSONObject.fromObject(jsonStr.trim());
							
							logS.setId(UUID.randomUUID().toString().replace("-", ""));
							logS.setCustomer_id(fromObject.get("customerId") == null ? null :fromObject.get("customerId").toString());
							logS.setMobilephone(fromObject.get("mobilephone") == null ? null :fromObject.get("mobilephone").toString());
							logS.setKey(fromObject.get("keyword") == null ? null :fromObject.get("keyword").toString());
							//时间
							String timeStr = fromObject.get("time") == null ? null :fromObject.get("time").toString();
							if(timeStr != null && timeStr.length()>0){
								String create_time = sdf.format(new Date(Long.valueOf(timeStr)));
								logS.setCreate_time(create_time);
								logS.setSimple_date(create_time.substring(0, 10));
							}
							
							//门店id
							String storeIdStr =fromObject.get("storeId") == null ? null :fromObject.get("storeId").toString() ;
							
							if(storeIdStr !=null && storeIdStr.length() >0){
								String[] storeArr = storeIdStr.split(",");
								String store_id = null;
								String other_store_id = null;
								for (String store_str : storeArr) {
									if(store_str.contains("F_")){
										logS.setFront_store_id(store_str.trim().replace("F_", ""));
										other_store_id = store_str.trim().replace("F_", "");
									}else if (store_str.contains("C_")){
										logS.setCloud_store_id(store_str.trim().replace("C_", ""));
										other_store_id = store_str.trim().replace("C_", "");
									}else{
										store_id = store_str.trim();
									}
								}
								if(store_id == null ){
									store_id = other_store_id;
								}
								logS.setStore_id(store_id);
							}
							
							//返回的商品id,每一个产生一条记录
							String productStr = fromObject.get("proId") == null ? null :fromObject.get("proId").toString() ;
							
							String[] productArr = productStr.split(",");
							if(productArr.length>0){
								
								
								String productNames = "";
								String productIds = "";
								//有返回的就进行拆解,每个返回值封装成一个对象
								for (String product_id : productArr) {
									if(product_id.length()>0){
										
										productIds +=product_id+",";
										String productName = productMap.get(product_id) == null ? null :productMap.get(product_id).toString();
										if(productName != null && productName.length()>0){
											productNames+= productName+",";
										}
									}
								}
								if(productNames!= null && productNames.length()>0){
									logS.setProduct_name(productNames.substring(0, productNames.length()-1));
								}
								if(productIds.length()>0){
									logS.setProduct_id(productIds.substring(0, productIds.length()-1));
								}
							}
							//添加到返回的集合类
							resultList.add(logS);
							
						}catch(JSONException e){
							 logger.error("JSONException   "+jsonStr);
						}catch(Exception e){
							e.printStackTrace();
							 logger.error(jsonStr);
						} 
					}
				}
				return resultList.iterator();
			}
		});
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, Log_Search.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_search");
		 } catch (AnalysisException e) {
			 logger.error("log_tmp_search 临时表创建失败");
			e.printStackTrace();
		 }
		 
		 if(isCreate){
			 //y有就删除
			 spark.sql("drop table if exists default."+tableName+" purge ").count();
			 //写入hive
			 spark.sql("create table default."+tableName+" stored as parquet as select * from log_tmp_search").count();
		 }else{
			 //写入hive
			 spark.sql("insert into  default."+tableName+"  select * from log_tmp_search").count();
		 }
		 
		 jsc.close();
		 spark.close();
		
		 logger.info("WechatLog运行完成--------------总用时："+Duration.between(start, Instant.now()).getSeconds()+"秒！");
		 return 0;
	}

}
