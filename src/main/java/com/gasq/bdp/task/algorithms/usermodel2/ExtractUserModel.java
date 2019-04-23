package com.gasq.bdp.task.algorithms.usermodel2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;

public class ExtractUserModel implements GasqSparkTask, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(ExtractUserModel.class);
	
	public static enum ACTION {
		Request("request", "f_customer_pro_request_new_action"), 	//请求行为
		Add("add", "f_customer_pro_add_new_action"), 			//加入购物车
		SuOrder("suorder", "f_customer_pro_suorder_new_action");		//成功交易订单
		private String action;
		private String table;
		ACTION(String action, String table) {
			this.action = action;
			this.table = table;
		}
		public String getAction() {
			return this.action;
		}
		public String getTable() {
			return this.table;
		}
		
	}
	
	private SparkSession spark;
	private int taskNum = 3;
	
	public ExtractUserModel() {
		if(spark == null) {
			spark = getHiveSpark("Extract User Model", false);
			JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
			spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sc.getConf().registerKryoClasses(new Class[]{com.gasq.bdp.task.algorithms.usermodel2.UserCommodityActionBean.class});
		}
	}

	public static void main(String[] args) {
		ExtractUserModel model = new ExtractUserModel();
		try {
			model.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	@Override
	public int run(String[] args) throws Exception {
		Instant startT = Instant.now();
		ACTION[] actions = ACTION.values();
		for(int i = 0; i < actions.length; i++) {
			Instant start = Instant.now();
			logger.warn("************begin to deal with::::" + actions[i].getAction());
			cleanData(actions[i].getTable(), (i == 0));
			Instant end = Instant.now();
			logger.warn("************end to cost::::" + Duration.between(start, end).toMillis() + "ms");
		}
		spark.close();
		Instant endT = Instant.now();
		logger.warn("Done work cost=" + Duration.between(startT, endT).toMillis() + "ms");
		return 0;
	}
	
	private void cleanData(String tableName, boolean isfirst) throws Exception {
		String action = getActionBy(tableName);
		if(StringUtils.isBlank(action)) {
			logger.warn(tableName + " HAVE NO action matches!!!");
			return;
		}
		String sql = "select * from " + tableName;
		Dataset<Row> rows = spark.sql(sql);
		List<StructField> fields = new ArrayList<>();
	    fields.add(DataTypes.createStructField("request_date", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("diffday", DataTypes.IntegerType, true));
	    fields.add(DataTypes.createStructField("customer_id", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("tag_level4_id", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("score", DataTypes.LongType, true));
	    StructType schema = DataTypes.createStructType(fields);
		Dataset<UserCommodityActionBean> beanSet = spark.createDataFrame(rows.toJavaRDD(), schema)
                .as(Encoders.bean(UserCommodityActionBean.class)).repartition(10).persist(StorageLevel.MEMORY_AND_DISK());
		int i = 0;
		while(i < taskNum) {
			beanSet = normalizeData(beanSet);
			i++;
		}
		beanSet.createOrReplaceTempView("user_model_temp");
		
		if(isfirst) {
			int result = (int)spark.sql("DROP TABLE IF EXISTS default.r_customer_pro_new_action").count();
//			System.out.println("************Result=" + result);
			spark.sql("CREATE TABLE IF NOT EXISTS default.r_customer_pro_new_action stored as parquet as select *, '" + action + "' as action from user_model_temp");
		} else {
			spark.sql("insert into default.r_customer_pro_new_action select *, '" + action + "' as action from user_model_temp");
		}
		beanSet.unpersist();
	}
	
	private Dataset<UserCommodityActionBean> normalizeData(Dataset<UserCommodityActionBean> beanSet) throws Exception {
//	    List<Integer> scoreList = beanSet.javaRDD().map(bean->Integer.parseInt(String.valueOf(bean.getScore()))).collect();             
//		Optional<Integer> sum = scoreList.stream().reduce(Integer::sum);
//		System.out.println("******************:" + scoreList.size());
//		Map<String, Double> recommend =  AlgorithmUtils.calculationProjectsRecommend(scoreList.size(), new BigDecimal(sum.get()), scoreList);
		//根据算法优化
		JavaRDD<Long> scoreRdd = beanSet.javaRDD().map(UserCommodityActionBean::getScore).cache();
		long size = scoreRdd.count();
		Long suml = scoreRdd.reduce(Long::sum);
		logger.info("---------------------计算完成(a_1+a_2+⋯+a_n)-->"+suml+"\t n--->"+size);
		BigDecimal sum = new BigDecimal(suml);
		double mv = sum.divide(new BigDecimal(size),6,BigDecimal.ROUND_HALF_UP).doubleValue();
		logger.info("---------------------计算完成 μ=(a_1+a_2+⋯+a_n)/n完成-->"+mv);
		double totalpow = scoreRdd.map(b ->  Math.pow(new Long(b).doubleValue() - mv, 2)).reduce(Double::sum);
		logger.info("---------------------计算完成(a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2-->"+totalpow);
		double sqrtv = new BigDecimal(Math.sqrt((new BigDecimal(totalpow)).divide(new BigDecimal(size),2,BigDecimal.ROUND_HALF_UP).doubleValue())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		logger.info("---------------------计算完成δ=√(((a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2)/n)-->"+sqrtv);
		
		logger.info("---------------------开始计算数据区间------------------------------");
		
		double min = new BigDecimal(mv-3*sqrtv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		double max = new BigDecimal(3*sqrtv+mv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		
		logger.info("---------------------计算数据区间完成(μ_1-〖3δ〗_1,3δ_1+μ_1)--->("+min+","+max+")");
		return beanSet.filter(bean -> bean.getScore() >= min && bean.getScore() <= max);
	}
	
	private String getActionBy(String tableName) {
		String action = "";
		if(tableName != null) {
			if(tableName.equalsIgnoreCase(ACTION.Request.getTable())) {
				action = ACTION.Request.getAction();
			} else if (tableName.equalsIgnoreCase(ACTION.Add.getTable())) {
				action = ACTION.Add.getAction();
			} else if (tableName.equalsIgnoreCase(ACTION.SuOrder.getTable())) {
				action = ACTION.SuOrder.getAction();
			}
		}
		return action;
	}

}
