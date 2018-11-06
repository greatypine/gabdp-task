/**
 * 
 */
package com.gasq.bdp.task.anzlyzer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.GasqSparkTask;

import scala.Tuple2;
/**
 * @author Ju_weigang
 * @时间 2018年5月14日下午5:44:14
 * @项目路径 com.gasq.bdp.logn
 * @描述 
 */
public class CreateClassTagIMonth1 implements GasqSparkTask {
	static Logger logger = LoggerFactory.getLogger(CreateClassTagIMonth.class);
	static final int partitionNum = 5;   
	static final String pattern = "[(.0-9)]+ml|\\【.*?\\】|/|[.0-9]+cm|[.0-9]+g|[.0-9]+G|[.0-9]+kg|[.0-9]+KG|[.0-9]+L|[.0-9]+箱|[.0-9]+平米|[.0-9]+天|[.0-9]+月|[.0-9]+日|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+袋|[.0-9]+卷|[.0-9]+组|[.0-9]+斤|[.0-9]+片|[.0-9]+套|[.0-9]+瓶|[.0-9]+份|[.0-9]+包|[.0-9]+套|[.0-9]+毫升|[.0-9]+ML|[.0-9]+支|[.0-9]+寸|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+英寸|[.0-9]+罐|[.0-9]+层|[.0-9]+张|[.0-9]+块|[.0-9]+mm|[.0-9]+枚|[.0-9]+个|[.0-9]+盒|[.0-9]+节|[.0-9]+粒|\\/袋|\\/组|嘻嘻哈哈\r\n" + 
    		"          |\\?|新品低至|五折|\\\\（.*?\\）|\\+|\\-|买一赠一|保税发货|组合|起发|积分商城|\\*+[0-9][0-9]|集采|\\*|嘻嘻哈哈\r\n" + 
    		"          |买一送一|请联系客服挑选|根据实际价格下单结算|请联系客服根据实际价格下单|根据实际金额下单|团购|随机赠送|首单体验|嘻嘻哈哈\r\n" + 
    		"          |当天下午三点前下单次日送达|订单备注颜色|备注需求颜色|订单备注颜色|北大荒|禾煜|惊爆价|套装|科控专享|门店活动下单专用|白领厨房|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+克|[.0-9]+元|[.0-9]+支装|\\(.*?\\)|嘻嘻哈哈\r\n" + 
    		"          \",\"|\\【|\\】|\\（|\\）|\\(|\\)|\\*|\\#|\\:|\\、|\"|\\,|\\.|\\!|\\_";
	static Long count = null;
	public static void main(String[] args) throws Exception{
		CreateClassTagIMonth1 task = new CreateClassTagIMonth1();
		task.run(args);
    }
    
	@Override
	public int run(String[] args) throws Exception {
    	Instant start = Instant.now();
//        if(args.length < 1){logger.info("<input data_path>");System.exit(-1);}
        String sql = "SELECT DISTINCT p.id,p.content_name,r.id channel_id FROM default.t_product p LEFT JOIN default.t_eshop e ON p.eshop_id = e.id LEFT JOIN default.t_department_channel r ON e.channel_id = r.id where r.name is not null limit 10";//数据集路径
        sql = "select * from t_class_month_i";
//        logger.info("输入参数为->"+StringUtils.join(args,","));
        SparkSession spark = getHiveSpark("CreateClassTagIMonth1",false);
//        spark.conf().set("spark.sql.broadcastTimeout", "36000");
//        spark.conf().set("spark.kryoserializer.buffer.max","256");
//        spark.conf().set("spark.kryoserializer.buffer","128");
//		spark.conf().set("spark.sql.codegen", "false");
//		spark.conf().set("spark.sql.inMemoryColumnarStorage.compressed", "false");
//		spark.conf().set("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
//		spark.conf().set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        Configuration hadoopConfiguration = sc.hadoopConfiguration();
//        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
//		HdfsFileUtil.removeHfile(hadoopConfiguration,args[0]);
		//加载数据，并将数据通过空格分割
		Dataset<Row> ds = spark.sql(sql).cache();
		JavaRDD<String> sku = ds.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row v1) throws Exception {
				String news = String.join(",", v1.getString(0),v1.getString(1),v1.getString(2));
				String content_name = v1.getString(1);
				String newv1 = content_name.replaceAll(pattern, "");
				Result result = ToAnalysis.parse(newv1);
				Object[] names = result.getTerms().stream().map(t->t.getName()).distinct().toArray(); //拿到terms
				String namesstr = StringUtils.join(names, "@@");
				String resultstr = String.join(",", news,namesstr);
				logger.warn("sku结果："+resultstr);
				return resultstr;
			}
		});
		/**
		 * 获取分词
		 */
		JavaRDD<String> sku_c = sku.flatMap(t->Arrays.asList(t.split(",")[3].split("@@")).iterator());
		sku_c.foreach(s->logger.warn("分词结果："+s));
		/**
		 * 计算分词频次
		 */
		JavaPairRDD<String, Integer> tagcounts = sku_c.mapToPair(t->new Tuple2<String, Integer>(t, 1)).reduceByKey((x,y)->x+y);
		tagcounts.foreach(t->logger.warn("计算分词频次结果："+t._1() + ": " + t._2()));
		long sku_count = sku.count();
		/**
		 * 计算分词标签出现的概率（p=log(n/1+m)） 
		 */
		JavaRDD<String> tag_p = tagcounts.map(v1->{
			Double p = new BigDecimal(Math.log(sku_count/1+v1._2)).setScale(4,BigDecimal.ROUND_HALF_UP).doubleValue();
			return String.join(",", v1._1,p.toString());
		});
		tag_p.foreach(f->logger.warn("计算分词标签出现的概率："+f));
		
		/**
		 * 转换tag和商品为一对多
		 */
		JavaRDD<String> class_tag = sku.flatMap(t->{
			String[] tags = t.split(",")[3].split("@@");
			List<String> array = new ArrayList<>();
			for (String tag : tags) {
				String join = String.join(",", t.split(",")[2],tag);
				array.add(join);
			}
			return array.iterator();
		});
		class_tag.foreach(f->logger.warn("转换标签："+f));
		/**
		 * 计算分类标签出现的次数
		 */
		JavaPairRDD<String, Integer> channeltagcounts = class_tag.mapToPair(t->new Tuple2<String, Integer>(t, 1)).reduceByKey((x,y)->x+y);
		channeltagcounts.foreach(t->logger.warn("计算分类标签出现的次数："+t._1() + ": " + t._2()));
		
		JavaRDD<Row> rdd_row = class_tag.map(f->RowFactory.create(f.split(",")[0],f.split(",")[1]));

	    List<StructField> fieldList = new ArrayList<StructField>();
	    fieldList.add(DataTypes.createStructField("channelid", DataTypes.StringType, true));
	    fieldList.add(DataTypes.createStructField("tag", DataTypes.StringType, true));
	    StructType structType = DataTypes.createStructType(fieldList);
	    spark.createDataFrame(rdd_row, structType).createOrReplaceTempView("class_channel_tag");
	    Dataset<Row> df_agg = spark.sql("select channelid,tag,count(tag) ct  from class_channel_tag group by channelid,tag");//去重后分组求和统计
	    df_agg.show();
		
//		
//		/**
//		 * 计算分类标签次数
//		 */
//		JavaPairRDD<String, Tuple2> channeltagpairRDD = channeltagcounts.mapToPair(t->new Tuple2(t._1.split(",")[0],new Tuple2<String,Integer>(t._1+"^"+t._2,2)));
//		/**
//		 * 合并分类次数和分类标签次数
//		 */
//		JavaPairRDD<String, Iterable<Tuple2>> groupByKey = channelpairRDD.union(channeltagpairRDD).partitionBy(new Partitioner() {
//			private static final long serialVersionUID = -1670557516209078158L;
//			@Override
//			public int numPartitions() {
//				return partitionNum;
//			}
//			@Override
//			public int getPartition(Object key) {
//				int hashCode = key.hashCode();
//				//因为hashCode可能产生负数,所以取绝对值
//				int absPartitions = Math.abs(hashCode % partitionNum);
//				return absPartitions;
//			}
//		}).groupByKey();
//		groupByKey.foreach(f->logger.warn("合并分类次数和分类标签次数："+f.toString()));
//		/**
//		 * 计算分词标签出现的概率（p=a/b） 
//		 * a=分类标签出现的次数
//		 * b=分类出现的次数
//		 */
//		JavaRDD<Tuple5<String,Integer,String, String, Double>> channelFrequency = groupByKey.map(new Function<Tuple2<String,Iterable<Tuple2>>, List<Tuple5<String,Integer,String, String, Double>>>() {
//			private static final long serialVersionUID = 1L;
//			@Override
//			public List<Tuple5<String,Integer,String, String, Double>> call(Tuple2<String, Iterable<Tuple2>> t) throws Exception {
//				Iterator<Tuple2> t2s = t._2.iterator();
//				Tuple2<String,Integer> a = null;
//				List<Tuple3<String,String,Integer>> bs = new ArrayList<>();
//				List<Tuple5<String,Integer,String,String,Double>> rest = new ArrayList<>();
//				while(t2s.hasNext()){
//					Tuple2<String,Integer> t2 = t2s.next();
//					if(t2._2()==1) {
//						a = new Tuple2<String,Integer>(t2._1.split("\\^")[0],Integer.parseInt(t2._1.split("\\^")[1]));
//					}else {
//						bs.add(new Tuple3<String,String,Integer>(t._1(),t2._1.split(",")[1].split("\\^")[0],Integer.parseInt(t2._1().split("\\^")[1])));
//					}
//				}
//				if(bs.size()>0) {
//					for (Tuple3<String,String,Integer> b : bs) {
//						Double p = new BigDecimal(b._3()).divide(new BigDecimal(a._2()),4,BigDecimal.ROUND_HALF_UP).doubleValue();
//						rest.add(new Tuple5<String,Integer,String,String,Double>(a._1,a._2,b._1(),b._2(),p));
//					}
//				}
//				return rest;
//			}
//		}).flatMap(f->f.iterator());
//		channelFrequency.foreach(f->logger.warn("计算分词标签出现的概率："+f.toString()));
		
		Instant end = Instant.now();
		logger.warn("CreateClassTagIMonth算法运行完成--------------总用时："+Duration.between(start, end).getSeconds()+"秒！");
		sc.close();
		return 0;
    }
    
}
