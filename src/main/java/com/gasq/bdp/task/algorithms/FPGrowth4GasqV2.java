/**
 * 
 */
package com.gasq.bdp.task.algorithms;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;
import com.gasq.bdp.task.util.HdfsFileUtil;

import scala.Tuple2;
/**
 * 关联分析算法
 * @author RenMian
 *
 */
public class FPGrowth4GasqV2 implements GasqSparkTask, Serializable {

	private static final long serialVersionUID = 1L;
	transient static Logger logger = LoggerFactory.getLogger(FPGrowth4GasqV2.class);
	static Long count = null;
	
	private static double defaultMinSupport = 0.005;//最小支持度
	private static int numPartition = 10;  //数据分区
	private static double minConfidence = 0.5;//最小置信度
	public static void main(String[] args) throws Exception{
    	FPGrowth4GasqV2 task = new FPGrowth4GasqV2();
    	task.run(args);
    }
    
	/**
	 * spark mlib fpgrowth计算关联
	 * @param key
	 * @param count
	 * @param minSupport
	 * @param items
	 * @return
	 */
    public static List<String> fpgrowth(String key, double count, double minSupport, JavaRDD<List<String>> items){
		//创建FPGrowth的算法实例，同时设置好训练时的最小支持度和数据分区，
		FPGrowth fpGrowth = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition);
		FPGrowthModel<String> model = fpGrowth.run(items);//执行算法
		//查看所有频繁集，并列出它出现的次数-->计算项集的support
		JavaPairRDD<String, Double> freqItems = model.freqItemsets().toJavaRDD().filter(fi -> fi.javaItems().size() <= 2).mapToPair(itemset -> {
			return new Tuple2<String, Double>(StringUtils.join(itemset.javaItems(),","), itemset.freq()/count); 
		});
		
		Map<String, Double> freqItemsSupport = freqItems.collectAsMap();
		
		//通过置信度筛选出强规则
		//antecedent表示前项
		//consequent表示后项
		//confidence表示规则的置信度
		List<String> fpGrowthResult = model.generateAssociationRules(minConfidence).toJavaRDD()
				.filter(v1 -> v1.javaAntecedent().size() == 1 && v1.javaConsequent().size() == 1)
				.map(v1 -> {
					String ss = null;
					List<String> javaAntecedent = v1.javaAntecedent();
					List<String> javaConsequent = v1.javaConsequent();
					double confidence = v1.confidence();
					List<String> javaac = new ArrayList<String>();
					javaac.addAll(javaAntecedent);
					javaac.addAll(javaConsequent);
					Double supportA_C = freqItemsSupport.get(StringUtils.join(javaac,","));	 //频繁集支持度
					if(supportA_C == null) {
						Collections.reverse(javaac);
						supportA_C = freqItemsSupport.get(StringUtils.join(javaac,","));
					}
					
					if(supportA_C != null && supportA_C > 0) {
						Double supportC = freqItemsSupport.get(StringUtils.join(javaConsequent,","));	//后项支持度：如果有频繁集，则后项支持度不可能为空
						double lift = confidence/supportC;
						String areaStr = getAreaStr(key);
						ss = String.join("\t", areaStr, StringUtils.join(javaAntecedent, ","),
								StringUtils.join(javaConsequent, ","), confidence + "", supportA_C + "", lift + "");
						logger.info("计算结果----》" + ss);
					}
					return ss;
				}).filter(ss -> ss != null).collect();

		logger.warn("区域《" + key + "》的频繁集----》" + "[" + fpGrowthResult.size() + "]");
		return fpGrowthResult;
    }
    
  	//area format:store_id, store_name, province_code, city_code, ad_code
	private static String getAreaStr(String store_id) {
		String areaStr = "";
		if (store_id.length() == 6) {
			if (store_id.substring(2).equals("0000")) {// province_code
				areaStr = String.join("\t", "null", "null", store_id, "null", "null");
			} else if (store_id.substring(4).equals("00")) {// city_code
				areaStr = String.join("\t", "null", "null", store_id.substring(0, 2) + "0000", store_id, "null");
			} else {
				areaStr = String.join("\t", "null", "null", store_id.substring(0, 2) + "0000",
						store_id.substring(0, 4) + "00", store_id);
			}
		} else {
			areaStr = String.join("\t", store_id, "null", "null", "null", "null");
		}
		return areaStr;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();      
        if(args.length < 1){logger.info("<input data_path>");System.exit(-1);}
        
//        String sql = "select temp.vst from(select concat(a.store_id,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.store_id )temp union all select temp1.vst from(select concat(a.province_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.province_code )temp1 union all select temp2.vst from(select concat(a.city_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.city_code )temp2 union all select temp3.vst from(select concat(a.ad_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.ad_code )temp3";//数据集路径
//        String sql = "select temp.vst from(select concat(a.store_id,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.store_id )temp where temp.ct>1 and temp.ct <=15 union all select temp1.vst from(select concat(a.province_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.province_code )temp1 where temp1.ct>1 and temp1.ct <=15 union all select temp2.vst from(select concat(a.city_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.city_code )temp2 where temp2.ct>1 and temp2.ct <=15 union all select temp3.vst from(select concat(a.ad_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.ad_code )temp3 where temp3.ct>1 and temp3.ct <=15";//数据集路径
        //test
//        String sql = "select temp.vst from(select concat(a.store_id,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct  from gabdp_user.m_apriori_data a where store_id='00000000000000000000000000000029' group by a.mykey,a.store_id)temp";//数据集路径
        
        if(args.length >= 2)defaultMinSupport = Double.parseDouble(args[1]);
        if(args.length >= 3)numPartition = Integer.parseInt(args[2]);
        if(args.length >= 4)minConfidence = Double.parseDouble(args[3]);
        logger.info("输入参数为->"+StringUtils.join(args,","));
        SparkSession spark = getHiveSpark("FPGrowth4GasqV2", false);	//true 为本地，false为集群。正式环境的设置为集群
        spark.conf().set("spark.sql.broadcastTimeout", "36000");
        spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.conf().set("spark.kryoserializer.buffer.max","256");
        spark.conf().set("spark.kryoserializer.buffer","128");
		spark.conf().set("spark.sql.codegen", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.compressed", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
		spark.conf().set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.getConf().registerKryoClasses(new Class[]{org.apache.spark.api.java.JavaSparkContext.class});
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[0]);
		//加载数据，并将数据通过空格分割
		
		String sql = "select key as _c0, vst as _c1 from ("
					+ "select a.province_code as key, concat_ws(',',collect_list(distinct a.item_id)) vst,count(distinct a.item_id) as ct from gabdp_user.m_apriori_data a group by a.mykey,a.province_code" 
					+ " union all "
					+ "select a.city_code as key, concat_ws(',',collect_list(distinct a.item_id)) vst,count(distinct a.item_id) as ct from gabdp_user.m_apriori_data a group by a.mykey,a.city_code"
					+ " union all "
					+ "select a.ad_code as key, concat_ws(',',collect_list(distinct a.item_id)) vst,count(distinct a.item_id) as ct from gabdp_user.m_apriori_data a group by a.mykey,a.ad_code"
					+ " union all "
					+ "select a.store_id as key, concat_ws(',',collect_list(distinct a.item_id)) vst,count(distinct a.item_id) as ct from gabdp_user.m_apriori_data a group by a.mykey,a.store_id"
					+ ") tmp";
		Dataset<Row> ds = spark.sql(sql);

//		Dataset<Row> ds = spark.read().csv("E://tmp//hello//query-hive-35949.csv");
		//构建数据
		JavaPairRDD<String, List<String>> rs = ds.sortWithinPartitions("_c0").javaRDD().mapToPair(row -> {	//兼容csv文本
			String key = row.getString(0);
			String items = row.getString(1);
			List<String> arrList =  new ArrayList<>();
			arrList.addAll(Arrays.asList(items.split(",")));
			return new Tuple2<String, List<String>>(key, arrList);
		});

		List<String> result = new ArrayList<String>();
		List<Tuple2<String, Iterable<List<String>>>> rsList = rs.groupByKey(numPartition).collect();
		for(Tuple2<String, Iterable<List<String>>> t : rsList) {	//每个店/区/市/省挨个分开计算
			Iterable<List<String>> _2 = t._2;
			List<List<String>> list = IteratorUtils.toList(_2.iterator());
			JavaRDD<List<String>> value = sc.parallelize(list);
			//minSupport应该随着购买商品的用户总量变化
			double minSupport = getMinSupportBy(list.size(), defaultMinSupport);
			logger.info("将javaPairRdd转换为listmap对象：key:"+t._1+"\t minSupport:"+ minSupport +"\t count:"+list.size());
			if(minSupport == 0.0) {
				logger.warn("!!!key:"+t._1+"\t计算基数太小不做关联分析计算！");
			} else {
				result.addAll(fpgrowth(t._1, (double)list.size(), minSupport, value));
			}
		}
		JavaRDD<String> distData = sc.parallelize(result);
		distData.coalesce(1).saveAsTextFile(args[0]);
//		distData.coalesce(1).saveAsTextFile("E://tmp//hello//result");
		Instant end = Instant.now();
		logger.warn("FPGrowth算法运行完成-------输出结果数----《"+result.size()+"》----总用时："+ Duration.between(start, end).toMinutes() +"分钟！");
		sc.close();
		return 0;
	}
	
	private double getMinSupportBy(int base, double defaultMinSupport) {
		if(base < 50) {
			return 0.0;
		} else if (base >= 50 && base < 200) {
			return 4 / (double)base;
		} else if (base >= 200 && base < 400) {
			return 0.05;
		} else if (base >= 400 && base < 1000) {
			return 0.01;
		} else if (base >= 1000 && base < 10000) {
			return defaultMinSupport;
		} else
			return 0.001;
	}
    
}
