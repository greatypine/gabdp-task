package com.gasq.bdp.task.algorithms.usermodel.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.db.HiveService;
import com.gasq.bdp.task.util.DelimiterType;


public class ReqFilterMap2 extends Mapper<NullWritable, Text, NullWritable, Text> {
	
	private Logger log = LoggerFactory.getLogger(ReqFilterMap2.class);
	
    @Override
	protected void setup(Mapper<NullWritable, Text, NullWritable, Text>.Context context) {
		try {
//			super.setup(context);
			String db = context.getConfiguration().get("db");
			String filepath = context.getConfiguration().get("db");
			String tablename = context.getConfiguration().get("reqtablename")+1;
			log.info("。。。。。。。。重新将已经清洗完成的数据生成为hive表。。。。。。文件路径："+filepath+"\t 表名称："+tablename);
			HiveService.loadFileInHive(db, filepath, tablename);
		} catch (Exception e) {
			new Exception("计算商品推荐方差值错处----",e);
		}
	}


    @Override
    protected void map(NullWritable key, Text value, Context context)throws IOException, InterruptedException {
    	double mininterval = Double.parseDouble(context.getConfiguration().get("mininterval"));
    	double maxinterval = Double.parseDouble(context.getConfiguration().get("maxinterval"));
    	String delimiter = DelimiterType.getValueByName(context.getConfiguration().get("delimiter"));
    	String[] vals = value.toString().split(delimiter);
		if(Double.parseDouble(vals[4])>mininterval && Double.parseDouble(vals[4])<maxinterval) {
			context.write(NullWritable.get(), value);
		}
    }
}