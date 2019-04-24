package com.gasq.bdp.task.algorithms.usermodel2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;

import scala.Tuple2;

/**
 * 提取用户数据模型，并清洗，去除异常数据值
 * @author RenMian
 *
 */
public class ExtractUserModel implements GasqSparkTask, Serializable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -3212332030469538560L;
	private static final Logger logger = LoggerFactory.getLogger(ExtractUserModel.class);
	public static final String DELIMER = "\t";
	
	public static enum ACTION {
		Request("request", "f_customer_pro_request_new_action", 0.1), 	//请求行为
		Add("add", "f_customer_pro_add_new_action", 1.0), 			//加入购物车
		SuOrder("suorder", "f_customer_pro_suorder_new_action", 0.5);		//成功交易订单
		private String action;
		private String table;
		private double alph;
		ACTION(String action, String table, double alph) {
			this.action = action;
			this.table = table;
			this.alph = alph;
		}
		public String getAction() {
			return this.action;
		}
		public String getTable() {
			return this.table;
		}
		public double getAlph() {
			return this.alph;
		}
	}
	
	private SparkSession spark;
	private int taskNum = 3;	//循环执行三次
	private Map<String, Double> alphMap;
	
	public ExtractUserModel() {
		if(spark == null) {
			spark = getHiveSpark("Extract User Model", false);
			JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
			spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sc.getConf().registerKryoClasses(new Class[]{com.gasq.bdp.task.algorithms.usermodel2.UserCommodityActionBean.class,
					com.gasq.bdp.task.algorithms.usermodel2.UserModelBean.class});
			alphMap = new HashMap<>();
		}
	}

	public static void main(String[] args) {
		if(args == null || args.length == 0 || StringUtils.isBlank(args[0])) {
			logger.error("arg0 is output path! arg1 is alph param format: request=0.1,add=1,suorder=0.5");
			return;
		}
		ExtractUserModel model = new ExtractUserModel();
		try {
			if(args != null && args.length == 2) {
				String alph = args[1];
				String[] alphs = alph.split(",");
				for(int i = 0; i < alphs.length; i++) {
					if(alphs[i] != null && alphs[i].contains("=")) {
						String[] str = alphs[i].split("=");
						if(str != null && str.length == 2 && StringUtils.isNumeric(str[1]))
							model.alphMap.put(str[0], Double.parseDouble(str[1]));
					}
					
				}
			}
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
			cleanData(actions[i].getTable(), (i==0));
			Instant end = Instant.now();
			logger.warn("************end to cost::::" + Duration.between(start, end).toMillis() + "ms");
		}
		//--------------------------merge数据并写到hdfs中
		logger.info("**********************begin to merge data");
		String sql = "select customer_id, tag_level4_id, sum(score) as score from default.r_customer_pro_new_action group by customer_id, tag_level4_id";
		Dataset<Row> dataset = spark.sql(sql);
		List<StructField> fields = new ArrayList<>();
	    fields.add(DataTypes.createStructField("customer_id", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("tag_level4_id", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("score", DataTypes.DoubleType, true));
	    StructType schema = DataTypes.createStructType(fields);
		spark.createDataFrame(dataset.toJavaRDD(), schema).as(Encoders.bean(UserModelBean.class))
				.map(UserModelBean::toString, Encoders.STRING()).coalesce(1).write().mode(SaveMode.Overwrite)
				.text(args[0]);
		spark.close();
		Instant endT = Instant.now();
		logger.warn("Done work cost=" + Duration.between(startT, endT).toMillis() +  "ms");
		return 0;
	}
	
	/**
	 * 清洗数据，默认清洗三次
	 * @param tableName
	 * @param isfirst
	 * @throws Exception
	 */
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
		
		
		double alph = getAlphByAction(action);
		logger.warn("!!!!!!!!get Action[" + action + "] Alph=" + alph);
		if(alph > 0) {
			JavaRDD<UserModelBean> beanKV = beanSet.javaRDD().mapToPair(f->{
				String newkey =  String.join(DELIMER,f.getCustomer_id(),f.getTag_level4_id());
	        	double vv = f.getScore() * Math.log(1+f.getDiffday()) * alph;
	        	return new Tuple2<String, Double>(newkey, vv);
			}).reduceByKey(Double::sum).map(f -> {
				String[] key = f._1.split(DELIMER);
				UserModelBean bean = new UserModelBean();
				bean.setCustomer_id(key[0]);
				bean.setTag_level4_id(key[1]);
	//			bean.setAction(action);
				bean.setScore(f._2);
				return bean;	
			});
			Dataset<Row> newBeanSet = spark.createDataFrame(beanKV, UserModelBean.class);
			newBeanSet.createOrReplaceTempView("user_model_temp");
			
			if(isfirst) {
				spark.sql("DROP TABLE IF EXISTS default.r_customer_pro_new_action").count();
	//			System.out.println("************Result=" + result);
				spark.sql("CREATE TABLE IF NOT EXISTS default.r_customer_pro_new_action stored as parquet as select *, '" + action + "' as action from user_model_temp");
			} else {
				spark.sql("insert into default.r_customer_pro_new_action select *, '" + action + "' as action from user_model_temp");
			}
		}
		
		
		//复写原有表
//		spark.sql("insert overwrite table " + tableName + " select request_date, diffday, customer_id, tag_level4_id, score from user_model_temp");
		beanSet.unpersist();
	}
	
	/**
	 * 去除数据异常值
	 * @param beanSet
	 * @return
	 * @throws Exception
	 */
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
	
	private double getAlphByAction(String action) {
		Double alph = this.alphMap.get(action);
		if(alph == null) {
			if(action.equals(ACTION.Request.getAction())) {
				alph = ACTION.Request.getAlph();
			} else if (action.equals(ACTION.Add.getAction())) {
				alph = ACTION.Add.getAlph();
			} else if (action.equals(ACTION.SuOrder.getAction())) {
				alph = ACTION.SuOrder.getAlph();
			}
		}
		return (alph == null) ? 0 : alph;
	}
	
	/*
	 * 根据table名称获取action
	 * @param tableName
	 * @return
	 */
	protected String getActionBy(String tableName) {
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
