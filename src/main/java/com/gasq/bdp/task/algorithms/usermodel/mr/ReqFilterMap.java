package com.gasq.bdp.task.algorithms.usermodel.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gasq.bdp.task.util.DelimiterType;

public class ReqFilterMap extends Mapper<LongWritable, Text, NullWritable, Text> {
//	private Logger log = LoggerFactory.getLogger(ReqFilterMap.class);
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)throws IOException, InterruptedException {
		super.cleanup(context);
//		try {
//			String db = context.getConfiguration().get("db");
//			String filepath = context.getConfiguration().get("filepath");
//			String tablename = context.getConfiguration().get("reqtablename");
//			log.info("。。。。。。。。重新将已经清洗完成的数据生成为hive表。。。。。。文件路径："+filepath+"\t 表名称："+tablename);
//			HiveService.loadFileInHive(db, filepath, tablename);
//		} catch (Exception e) {
//			new Exception("重新将已经清洗完成的数据生成为hive表错处----",e);
//		}
	}


	
    @Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) {
//		try {
////			super.setup(context);
//			String tablename = context.getConfiguration().get("reqtablename");
//			AlgorithmUtils au = new AlgorithmUtils();
//			Map<String, Double> recommend = au.calculationProjectsRecommend(tablename);
//			if(recommend!=null && recommend.get("mininterval")!=0 && recommend.get("maxinterval")!=0) {
//				context.getConfiguration().set("mininterval", recommend.get("mininterval").toString());
//				context.getConfiguration().set("maxinterval", recommend.get("maxinterval").toString());
//			}
//		} catch (Exception e) {
//			new Exception("计算商品推荐方差值错处----",e);
//		}
	}


    @Override
    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
    	double mininterval = Double.parseDouble(context.getConfiguration().get("mininterval"));
    	double maxinterval = Double.parseDouble(context.getConfiguration().get("maxinterval"));
    	String delimiter = DelimiterType.getValueByName(context.getConfiguration().get("delimiter"));
    	String[] vals = value.toString().split(delimiter);
		if(Double.parseDouble(vals[4])>mininterval && Double.parseDouble(vals[4])<maxinterval) {
			context.write(NullWritable.get(), value);
		}
    }
    
    
}