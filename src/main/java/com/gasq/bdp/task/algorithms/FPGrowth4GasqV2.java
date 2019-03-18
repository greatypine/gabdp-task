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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
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
 * @author Ju_weigang
 * @时间 2018年5月14日下午5:44:14
 * @项目路径 com.gasq.bdp.logn
 * @描述 
 */
public class FPGrowth4GasqV2 implements GasqSparkTask, Serializable {

	private static final long serialVersionUID = 1L;
	transient static Logger logger = LoggerFactory.getLogger(FPGrowth4GasqV2.class);
	static Long count = null;
	public static void main(String[] args) throws Exception{
    	FPGrowth4GasqV2 task = new FPGrowth4GasqV2();
    	task.run(args);
    }
    
    private static List<String> fpgrowth(JavaSparkContext sc,JavaRDD<List<String>> value,String key,double minSupport,int numPartition,double minConfidence, Broadcast<Map<String, Long>> broadcast){
    	String[] keyandcount = key.split("\\|");
    	String store_id = keyandcount[0];
    	double count = Double.parseDouble(keyandcount[1].toString());
		//创建FPGrowth的算法实例，同时设置好训练时的最小支持度和数据分区，
		FPGrowth fpGrowth = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition);
		FPGrowthModel<String> model = fpGrowth.run(value);//执行算法
		//查看所有频繁集，并列出它出现的次数
		List<FreqItemset<String>> collect2 = model.freqItemsets().toJavaRDD().filter(fi -> fi.javaItems().size()<=2).collect();
		collect2.forEach(itemset -> {
			logger.info("门店《"+key+"》的频繁集----》"+"[" + itemset.javaItems() + "]," + itemset.freq());
    		Map<String, Long> acdata1 = broadcast.value();
    		acdata1.put(StringUtils.join(itemset.javaItems(),","), itemset.freq());
		});
		
		//通过置信度筛选出强规则
		//antecedent表示前项
		//consequent表示后项
		//confidence表示规则的置信度
		List<String> fpGrowthResult = new ArrayList<String>();
		List<Rule<String>> collect = model.generateAssociationRules(minConfidence).toJavaRDD()
				.filter(v1 -> v1.javaAntecedent().size() == 1 && v1.javaConsequent().size() == 1).collect();
		for (Rule<String> v1 : collect) {
			List<String> javaAntecedent = v1.javaAntecedent();
			List<String> javaConsequent = v1.javaConsequent();
			double confidence = v1.confidence();
			Map<String, Long> acdata1 = broadcast.value();
			List<String> javaac = new ArrayList<String>();
			javaac.addAll(javaAntecedent);
			javaac.addAll(javaConsequent);
			Long freq = acdata1.get(StringUtils.join(javaac,","));
			if(freq==null) {
				Collections.reverse(javaac);
				freq = acdata1.get(StringUtils.join(javaac,","));
			}
			if(freq!=null) {
				if(freq>0) {
					double sup = freq/count;
					Long pCFreq = acdata1.get(StringUtils.join(javaConsequent,","));
					double lift = confidence/(pCFreq/count);
					//area format:store_id, store_name, province_code, city_code, ad_code
					String areaStr = "";
					if(store_id.length() == 6) {
						if(store_id.substring(2).equals("0000")) {//province_code
							areaStr = String.join("\t", "null", "null", store_id, "null", "null");
						} else if (store_id.substring(4).equals("00")) {//city_code
							areaStr = String.join("\t", "null", "null", store_id.substring(0,2)+"0000",store_id,"null");
						} else {
							areaStr = String.join("\t", "null", "null",store_id.substring(0,2)+"0000",store_id.substring(0,4)+"00", store_id);
						}
					} else {
						areaStr = String.join("\t", store_id, "null", "null", "null", "null");
					}
					String ss = String.join("\t",areaStr,StringUtils.join(javaAntecedent,","),StringUtils.join(javaConsequent,","),confidence+"",sup+"",lift+"");
					fpGrowthResult.add(ss);
					logger.info("计算结果----》"+ss);
				}
			}
		}
		logger.warn("门店《"+store_id+"》的频繁集----》"+"[" + fpGrowthResult.size() + "]");
		Map<String, Long> acdata1 = broadcast.value();
		acdata1.clear();
		return fpGrowthResult;
    }

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
    	final List<String> result = new ArrayList<String>();
    	final Map<String,Long> acdata = new HashMap<String,Long>();
        double defaultMinSupport = 0.005;//最小支持度
        int numPartition = 4;  //数据分区
        double minConfidence = 0.5;//最小置信度
        if(args.length < 1){logger.info("<input data_path>");System.exit(-1);}
        String sql = "select temp.vst from(select concat(a.store_id,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.store_id )temp union all select temp1.vst from(select concat(a.province_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.province_code )temp1 union all select temp2.vst from(select concat(a.city_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.city_code )temp2 union all select temp3.vst from(select concat(a.ad_code,'\\t',concat_ws(',',collect_list(distinct a.item_id))) vst,count(1)as ct from gabdp_user.m_apriori_data a group by a.mykey,a.ad_code )temp3";//数据集路径
        if(args.length >= 2)defaultMinSupport = Double.parseDouble(args[1]);
        if(args.length >= 3)numPartition = Integer.parseInt(args[2]);
        if(args.length >= 4)minConfidence = Double.parseDouble(args[3]);
        logger.info("输入参数为->"+StringUtils.join(args,","));
        SparkSession spark = getHiveSpark("FPGrowth4GasqV2", false);	//true 为本地，false为集群。正式环境的设置为集群
        spark.conf().set("spark.sql.broadcastTimeout", "36000");
        spark.conf().set("spark.kryoserializer.buffer.max","256");
        spark.conf().set("spark.kryoserializer.buffer","128");
		spark.conf().set("spark.sql.codegen", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.compressed", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
		spark.conf().set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		final Broadcast<List<String>> broadcastResult = sc.broadcast(result);
        final Broadcast<Map<String, Long>> broadcastacdata = sc.broadcast(acdata);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[0]);
		//加载数据，并将数据通过空格分割
		Dataset<Row> ds = spark.sql(sql);
		
		JavaPairRDD<String, List<String>> transactions = ds.javaRDD().mapToPair(new PairFunction<Row, String, List<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, List<String>> call(Row row) throws Exception {
				logger.info("传入的ROW对象为："+row.mkString());
				String[] vs = row.getString(0).toString().trim().split("\t");
  				String key = vs[0].toString().trim().replaceAll(" ", "");
  				String[] v = vs[1].toString().trim().replaceAll(" ", "").split(",");
  				String vv = StringUtils.join(v," ");
  				logger.info("转换为JavaPairRDD<k,v>对象：key:"+key+"\t value:"+vv);
  				//NOTE！！！List<String>应该没有重复数据，否则会抛org.apache.spark.SparkException: Items in a transaction must be unique but got WrappedArray
  				//目前做法是构造数据时就进行去重，也可以放到程序中去处理
  				List<String> arrList =  Arrays.asList(v);
  				return new Tuple2<String, List<String>>(key, arrList);
			}
		 });
		
        List<Tuple2<String, Iterable<List<String>>>> collect = transactions.groupByKey().collect();
        for (Tuple2<String, Iterable<List<String>>> t : collect) {
        	String _1 = t._1;
			Iterable<List<String>> _2 = t._2;
			List<List<String>> list = IteratorUtils.toList(_2.iterator());
			String key = _1+"|"+list.size();
			JavaRDD<List<String>> value = sc.parallelize(list);	
			List<String> value2 = broadcastResult.getValue();
			//minSupport应该随着购买商品的用户总量变化
			double minSupport = getMinSupportBy(list.size(), defaultMinSupport);
			logger.info("将javaPairRdd转换为listmap对象：key:"+_1+"\t minSupport:"+ minSupport +"\t count:"+list.size());
			if(minSupport == 0.0) {
				logger.warn("!!!key:"+_1+"\t计算基数太小不做关联分析计算！");
			} else {
				value2.addAll(fpgrowth(sc,value,key,minSupport,numPartition,minConfidence,broadcastacdata));
			}
		}
		JavaRDD<String> distData = sc.parallelize(result);
		distData.coalesce(1).saveAsTextFile(args[0]);
		Instant end = Instant.now();
		logger.warn("FPGrowth算法运行完成-------输出结果数----《"+result.size()+"》----总用时："+ Duration.between(start, end).toMinutes() +"分钟！");
		sc.close();
		return 0;
	}
	
	private double getMinSupportBy(int base, double defaultMinSupport) {
		if(base < 10) {
			return 0.0;
		} else if (base >= 10 && base < 200) {
			return 0.1;
		} else if (base >= 200 && base < 1000) {
			return 0.01;
		} else if (base >= 1000) {
			return defaultMinSupport;
		}
		return defaultMinSupport;
	}
    
}
