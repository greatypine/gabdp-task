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
import org.apache.hadoop.conf.Configuration;
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
import org.apache.spark.storage.StorageLevel;
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
public class CreateClassTagIMonth implements GasqSparkTask{
	static Logger logger = LoggerFactory.getLogger(CreateClassTagIMonth.class);
	static final int partitionNum = 5;
	static String fileoutpath = "/user/juwg/test";

	static final String pattern = "[(.0-9)]+ml|\\【.*?\\】|/|[.0-9]+cm|[.0-9]+g|[.0-9]+G|[.0-9]+kg|[.0-9]+KG|[.0-9]+L|[.0-9]+箱|[.0-9]+平米|[.0-9]+天|[.0-9]+月|[.0-9]+日|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+袋|[.0-9]+卷|[.0-9]+组|[.0-9]+斤|[.0-9]+片|[.0-9]+套|[.0-9]+瓶|[.0-9]+份|[.0-9]+包|[.0-9]+套|[.0-9]+毫升|[.0-9]+ML|[.0-9]+支|[.0-9]+寸|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+英寸|[.0-9]+罐|[.0-9]+层|[.0-9]+张|[.0-9]+块|[.0-9]+mm|[.0-9]+枚|[.0-9]+个|[.0-9]+盒|[.0-9]+节|[.0-9]+粒|\\/袋|\\/组|嘻嘻哈哈\r\n" + 
    		"          |\\?|新品低至|五折|\\\\（.*?\\）|\\+|\\-|买一赠一|保税发货|组合|起发|积分商城|\\*+[0-9][0-9]|集采|\\*|嘻嘻哈哈\r\n" + 
    		"          |买一送一|请联系客服挑选|根据实际价格下单结算|请联系客服根据实际价格下单|根据实际金额下单|团购|随机赠送|首单体验|嘻嘻哈哈\r\n" + 
    		"          |当天下午三点前下单次日送达|订单备注颜色|备注需求颜色|订单备注颜色|北大荒|禾煜|惊爆价|套装|科控专享|门店活动下单专用|白领厨房|嘻嘻哈哈\r\n" + 
    		"          |[.0-9]+克|[.0-9]+元|[.0-9]+支装|\\(.*?\\)|嘻嘻哈哈\r\n" + 
    		"          \",\"|\\【|\\】|\\（|\\）|\\(|\\)|\\*|\\#|\\:|\\、|\"|\\,|\\.|\\!|\\_|[0-9]|\\：|\\[|\\]|\\／|\\《|\\》|kg|m|x|\\，|\\。|\\？|\\%|\\”|\\“|\\·|b|p|s|l|d|\\￥|a|\\——";
	static Long count = null;
	public static void main(String[] args) throws Exception{
//		args = new String[2];
//		args[0] = "t_class_month_i";
//		args[1] = "hdfs://10.10.20.19:8020/user/juwg/test";
		CreateClassTagIMonth task = new CreateClassTagIMonth();
    	task.run(args);
    }
    
	@Override
	public int run(String[] args) throws Exception {
    	Instant start = Instant.now();
//        if(args.length < 1){logger.info("<input data_path>");System.exit(-1);}
        String sql = "";//数据集路径
        sql = "select * from "+args[0];
//        logger.info("输入参数为->"+StringUtils.join(args,","));
        SparkSession spark = getHiveSpark("CreateClassTagIMonth",false);
        spark.conf().set("spark.sql.broadcastTimeout", "36000");
        spark.conf().set("spark.kryoserializer.buffer.max","256");
        spark.conf().set("spark.kryoserializer.buffer","128");
		spark.conf().set("spark.sql.codegen", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.compressed", "false");
		spark.conf().set("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
		spark.conf().set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
		HdfsFileUtil.removeHfile(hadoopConfiguration,args[1]);
		//加载数据，并将数据通过空格分割
		Dataset<Row> ds = spark.sql(sql).cache();
		ds.createOrReplaceTempView("sku");
		JavaRDD<String> sku = ds.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row v1) throws Exception {
				String news = String.join(",", v1.getString(0),v1.getString(1),v1.getString(2));
				String content_name = v1.getString(1);
//				if(content_name.equals("康复辅具租赁")) content_name="康复辅具租赁菜鸟快递";
				String newv1 = content_name.trim().replaceAll(pattern, "");
				Result result = ToAnalysis.parse(newv1);
				Object[] names = result.getTerms().stream().map(t->t.getName().trim().replaceAll(pattern, "")).filter(f->StringUtils.isNotBlank(f)).distinct().toArray(); //拿到terms
				String namesstr = StringUtils.join(names, "@@");
				String resultstr = String.join(",", news,namesstr);
				logger.warn("sku结果："+resultstr);
				return resultstr;
			}
		}).filter(f->StringUtils.isNotBlank(f.toString()));
		/**
		 * 获取分词
		 */
		JavaRDD<String> sku_c = sku.flatMap(t->Arrays.asList((t.split(",").length>3)?t.split(",")[3].split("@@"):new String[] {}).iterator()).filter(f->StringUtils.isNotBlank(f.toString()));;
//		sku_c.foreach(s->logger.warn("分词结果："+s));
		/**
		 * 计算分词频次
		 */
		JavaPairRDD<String, Integer> tagcounts = sku_c.mapToPair(t->new Tuple2<String, Integer>(t, 1)).reduceByKey((x,y)->x+y);
//		tagcounts.foreach(t->logger.warn("计算分词频次结果："+t._1() + ": " + t._2()));
		long sku_count = sku.count();
		/**
		 * 计算分词标签出现的概率（p=log(n/1+m)） 
		 */
		JavaRDD<String> tag_p = tagcounts.map(v1->{
			Double p = new BigDecimal(Math.log(sku_count/(1+v1._2))).setScale(4,BigDecimal.ROUND_HALF_UP).doubleValue();
			return String.join(",", v1._1,p.toString());
		});
//		tag_p.foreach(f->logger.warn("计算分词标签出现的概率："+f));
		JavaRDD<Row> tagcountmap = tag_p.map(f->RowFactory.create(f.split(",")[0],Double.parseDouble(f.split(",")[1])));
		List<StructField> tagcountfieldList = new ArrayList<StructField>();
		tagcountfieldList.add(DataTypes.createStructField("tag", DataTypes.StringType, false));
		tagcountfieldList.add(DataTypes.createStructField("p", DataTypes.DoubleType, true));
		StructType tagcountstructType = DataTypes.createStructType(tagcountfieldList);
		spark.createDataFrame(tagcountmap,tagcountstructType).createOrReplaceTempView("tag_p");
		Dataset<Row> tag_p_row = spark.sql("select * from tag_p");
		tag_p_row.persist(StorageLevel.NONE());
//		tag_p_row.show(false);
		/**
		 * 转换tag和商品为一对多
		 */
		JavaRDD<String> class_tag = sku.flatMap(t->{
			List<String> array = new ArrayList<>();
			if(t.split(",").length>3) {
				String[] tags = t.split(",")[3].split("@@");
				for (String tag : tags) {
					if(StringUtils.isNotBlank(tag)) {
						String join = String.join(",", t.split(",")[2],tag);
						if(StringUtils.isNotBlank(join)) array.add(join);
					}
				}
			}
			return array.iterator();
		});
//		class_tag.foreach(f->logger.warn("转换标签："+f));
		/**
		 * 将channel和tag转换为表
		 */
		JavaRDD<Row> class_channel_tag_row = class_tag.map(f->RowFactory.create(f.split(",")[0],f.split(",")[1]));
		List<StructField> fieldList = new ArrayList<StructField>();
		fieldList.add(DataTypes.createStructField("channelid", DataTypes.StringType, true));
		fieldList.add(DataTypes.createStructField("tag", DataTypes.StringType, true));
		StructType structType = DataTypes.createStructType(fieldList);
		spark.createDataFrame(class_channel_tag_row, structType).createOrReplaceTempView("class_channel_tag");
		/**
		 * 计算分类标签在所有分类中出现的次数
		 */
	    Dataset<Row> class_channel_tag = spark.sql("select concat(channelid,'&',tag) as channelAndTag,channelid,tag,count(*) channelAndTagCount  from class_channel_tag group by channelid,tag");//去重后分组求和统计
	    class_channel_tag.createOrReplaceTempView("class_channel_and_tag_count");
	    class_channel_tag.persist(StorageLevel.NONE());
	    spark.sql("select * from class_channel_and_tag_count").show(false);
//		JavaPairRDD<String, Integer> channeltagcounts = class_tag.mapToPair(t->new Tuple2<String, Integer>(t, 1)).reduceByKey((x,y)->x+y);
//		channeltagcounts.foreach(t->logger.warn("计算分类标签出现的次数："+t._1() + ": " + t._2()));
		/**
		 * 计算分类标签在分类中出现得次数
		 */
	    Dataset<Row> class_channel = spark.sql("select channel_id as channelid,count(*) channelCount from sku group by channel_id");//去重后分组求和统计
	    class_channel.persist(StorageLevel.NONE());
	    class_channel.createOrReplaceTempView("class_channel_count");
//	    spark.sql("select * from class_channel_count").show(false);
	    Dataset<Row> class_channel_tag_p = spark.sql("select a.channelid,b.channelAndTag,b.channelAndTagCount,a.channelCount,b.tag,cast((cast(b.channelAndTagCount as double) /cast(a.channelCount as double)) as decimal(18,4)) as p from class_channel_count a left join class_channel_and_tag_count b on a.channelid = b.channelid ");
//	    class_channel_tag_p.show(false);
	    class_channel_tag_p.createOrReplaceTempView("class_channel_tag_p");
	    class_channel_tag_p.persist(StorageLevel.NONE());
	    Dataset<Row> class_tag_i = spark.sql("select p.channelAndTag,p.channelAndTagCount,p.channelid,p.tag,cast(p.p*(t.p+p.channelAndTagCount) as decimal(18,4)) as i from class_channel_tag_p as p left join tag_p t on p.tag = t.tag");
//	    class_tag_i.show(false);
	    JavaRDD<String> map = class_tag_i.toJavaRDD().map(f->String.join(",",f.getString(0),f.get(1).toString(),f.getString(2),f.getString(3),f.get(4).toString()));
	    map.coalesce(1).saveAsTextFile(args[1]);
		Instant end = Instant.now();
		logger.warn("CreateClassTagIMonth算法运行完成--------------总用时："+Duration.between(start, end).getSeconds()+"秒！");
		sc.close();
		return 0;
    }
}
