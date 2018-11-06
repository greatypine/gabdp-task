package com.gasq.bdp.task.algorithms.usermodel.mr;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.gasq.bdp.task.algorithms.mahout.CustomCombineFileInputFormat;
import com.gasq.bdp.task.util.DelimiterType;
import com.gasq.bdp.task.util.HdfsFileUtil;


public class CustomerReqActionFilter {
	static double alph = 0.0;
	private static String reqtablename = "f_customer_pro_request_new_action";
	private static String db = "default";
	private static String inputurl="/user/hive/warehouse/"+reqtablename;
	private static String outputurl="/user/algorithm/test/"+reqtablename;
	private static String delimiter=DelimiterType.valueOfName(DelimiterType.defaultHive.getValue());
	private static int reducer=1;
	
	public static void main(String[] args) throws Exception {
//    	System.out.println("CustomerReqAction传入参数：参数1=："+args[0]+"\t"+args[1]+"\t"+args[2]);
		Instant start = Instant.now();
		try {
			Configuration conf = new Configuration();
			conf.set("filePathUri", inputurl);
			conf.set("delimiter", delimiter);
			conf.set("reqtablename", reqtablename);
			conf.set("db", db);
			conf.set("filepath", outputurl);
			
			Job job = Job.getInstance(conf);
			job.setJarByClass(CustomerReqActionFilter.class);
			ChainMapper.addMapper(job,ReqFilterMap.class, LongWritable.class,Text.class, NullWritable.class,Text.class,conf);
            ChainMapper.addMapper(job,ReqFilterMap2.class,NullWritable.class, Text.class,NullWritable.class,Text.class,conf);
//            ChainReducer.setReducer(job,ReducerImpl.class, Text.class,Text.class, Text.class, Text.class, conf);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(reducer);
			job.setInputFormatClass(CustomCombineFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path inpath = new Path(inputurl);
			Path outpath = new Path(outputurl);
			HdfsFileUtil.removeHfile(conf, outputurl);
			FileInputFormat.addInputPath(job, inpath);
			FileOutputFormat.setOutputPath(job, outpath);
			Instant end = Instant.now();
			System.out.println("CustomerReqActionFilter计算用时："+Duration.between(start, end).toMillis()+"ms！");
			System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
	
}