/**
 * 
 */
package com.gasq.bdp.task.algorithms.logisticRegression;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
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

/**
 * @author Ju_weigang
 * @时间 2018年8月17日上午10:05:53
 * @项目路径 com.gasq.bdp.task.algorithms.logisticRegression
 * @描述 
 */
public class SparkMLLogisticRegression implements GasqSparkTask, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(SparkMLLogisticRegression.class);
	public static void main(String[] args) {
		try {
			SparkMLLogisticRegression logisticRegression = new SparkMLLogisticRegression();
			logisticRegression.run(args);
		} catch (Exception e) {
		}
	}

	@SuppressWarnings("resource")
	@Override
	public int run(String[] args) throws Exception {
		Instant start = Instant.now();
		SparkSession spark = getHiveSpark("LogisticRegression",true);
		SparkContext sc = new JavaSparkContext(spark.sparkContext()).sc();
		//通过MLUtils工具类读取LIBSVM格式数据集
		RDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc,"D:/usr/testSet.txt");
		//打印总数目
		logger.info("data Count:"+data.count());
		double[] array = {0.7,0.3};
		RDD<LabeledPoint>[] result = data.randomSplit(array,2L);
		JavaRDD<LabeledPoint> training = result[0]. toJavaRDD();
		logger.info("training Count:"+training.count());
		JavaRDD<LabeledPoint> test = result[1].toJavaRDD();
		logger.info("test Count:"+test.count());
		//打印训练数据数目   
		logger.info("training Count:"+result.length);
		//发现测试集和训练集并不一定按1：9的比例分
		//建立LogisticRegressionWithLBFGS对象，设置分类数 3 ，run传入训练集开始训练，返回训练后的模型
		LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(data);
		//使用训练后的模型对测试集进行测试，同时打印标签和测试结果
		JavaRDD<Row> jrow = test.map(p->RowFactory.create(p.label(), model.predict(p.features())));
		double count = jrow.filter(f->f.getDouble(0)==f.getDouble(1)).count();
		double total = jrow.count();
		logger.info("------------------------精度： " + count/total);
		//jrow.foreach(f->System.out.println(f));
		List<StructField> tagcountfieldList = new ArrayList<StructField>();
		tagcountfieldList.add(DataTypes.createStructField("label", DataTypes.DoubleType, true));
		tagcountfieldList.add(DataTypes.createStructField("features", DataTypes.DoubleType, true));
		StructType tagcountstructType = DataTypes.createStructType(tagcountfieldList);
		spark.createDataFrame(jrow,tagcountstructType).createOrReplaceTempView("tag_p");
		Dataset<Row> tag_p_row = spark.sql("select * from tag_p");
		tag_p_row.persist(StorageLevel.NONE());
		tag_p_row.show(false);
		Instant end = Instant.now();
		logger.warn("CreateClassTagIMonth算法运行完成--------------总用时："+Duration.between(start, end).getSeconds()+"秒！");
		spark.close();
		return 0;
	}
	
}
