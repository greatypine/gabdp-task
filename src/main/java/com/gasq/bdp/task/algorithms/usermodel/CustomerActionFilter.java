package com.gasq.bdp.task.algorithms.usermodel;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.algorithms.usermodel.mr.MaxMapper;
import com.gasq.bdp.task.util.AlgorithmUtils;
import com.gasq.bdp.task.util.DelimiterType;
import com.gasq.bdp.task.util.HdfsFileUtil;


public class CustomerActionFilter {
	
	private static Logger log = LoggerFactory.getLogger(MaxMapper.class);
	private static String tablename1 = "";
	
//	private static String db = "default";
	private static String inputurl1 = "/user/hive/warehouse/";
	private static String outputurl1 = "/user/algorithm/test/";
	
	private static String tablename2 = "";
	private static String inputurl2 = "";
	private static String outputurl2 ="/user/algorithm/test/";
	
	private static String tablename3 = "";
	private static String inputurl3 = "";
	private static String outputurl3 = "/user/algorithm/test/";
	
	private static String delimiter=DelimiterType.valueOfName(DelimiterType.defaultHive.getValue());
	private static int reducer=1;
	
	public static class Map1 extends Mapper<Writable, Text, NullWritable, Text> {
	        // 实现map函数
	    public void map(Writable key,Text value, Context context)throws IOException, InterruptedException {
	    	context.write(NullWritable.get(), value);
	    }
	}

    public static class Reduce1 extends Reducer<NullWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, NullWritable, Text>.Context context)throws IOException, InterruptedException {
			int index = 0;
			BigDecimal sum = BigDecimal.ZERO;
			List<Integer> cns = new ArrayList<Integer>();
			List<String> myList = new ArrayList<String>();
			for (Text value:values) {
				String val = value.toString();
				String[] beans = val.split(DelimiterType.getValueByName(context.getConfiguration().get("delimiter")));
				int clicknumb = Integer.parseInt(beans[4]);
				if(clicknumb>0) {
					index++;
					sum = sum.add(new BigDecimal(clicknumb));
					cns.add(clicknumb);
					myList.add(val);
				}
			}
			Map<String, Double> recommend = null;
			try {
				recommend = AlgorithmUtils.calculationProjectsRecommend(index, sum, cns);
			} catch (Exception e) {
				e.printStackTrace();
			}
			double mininterval = Double.parseDouble(recommend.get("mininterval").toString());
			double maxinterval = Double.parseDouble(recommend.get("maxinterval").toString());
			if(recommend!=null && recommend.get("mininterval")!=0 && recommend.get("maxinterval")!=0) {
				context.getConfiguration().set("mininterval", recommend.get("mininterval").toString());
				context.getConfiguration().set("maxinterval", recommend.get("maxinterval").toString());
				for (String text : myList) {
					String[] beans = text.split(DelimiterType.getValueByName(context.getConfiguration().get("delimiter")));
					int clicknumb = Integer.parseInt(beans[4]);
					if(clicknumb>mininterval && clicknumb<maxinterval) {
						context.write(NullWritable.get(), new Text(text));
					}
				}
			}
		}
    }
    
	public static void main(String[] args) throws Exception {
//		tablename1 = "f_customer_pro_request_new_action";
		configureArgs(args);
		log.info("..........................."+tablename1+"准备开始清洗.............................");
		Instant start = Instant.now();
		Instant start1 = Instant.now();
    	Configuration conf1 = new Configuration();
    	conf1.set("delimiter", delimiter);
    	Job job1 = Job.getInstance(conf1,"job1");
    	job1.setJarByClass(CustomerActionFilter.class);
    	job1.setMapperClass(Map1.class);
    	job1.setReducerClass(Reduce1.class);
    	job1.setMapOutputKeyClass(NullWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		Path inpath1 = new Path(inputurl1);
		Path outpath1 = new Path(outputurl1);
		job1.setNumReduceTasks(reducer);
		HdfsFileUtil.removeHfile(conf1, outputurl1);
		FileInputFormat.addInputPath(job1, inpath1);
		FileOutputFormat.setOutputPath(job1, outpath1);
    	job1.waitForCompletion(true);
    	Instant end = Instant.now();
    	log.info(tablename1+"清洗第1次耗时："+Duration.between(start, end).toMillis()+"ms！");
    	
    	//second mapreduce
    	
    	start = Instant.now();
    	Configuration conf2 = new Configuration();
    	conf2.set("delimiter", delimiter);
    	Job job2 = Job.getInstance(conf2,"job2");
    	job2.setJarByClass(CustomerActionFilter.class);
    	job2.setMapperClass(Map1.class);
    	job2.setReducerClass(Reduce1.class);
    	job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
    	job2.setNumReduceTasks(reducer);
		Path inpath2 = new Path(inputurl2);
		Path outpath2 = new Path(outputurl2);
		HdfsFileUtil.removeHfile(conf2, outputurl2);
		FileInputFormat.addInputPath(job2, inpath2);
		FileOutputFormat.setOutputPath(job2, outpath2);
    	job2.waitForCompletion(true);
        end = Instant.now();
        log.info(tablename1+"清洗第2次耗时："+Duration.between(start, end).toMillis()+"ms！");
        //third mapreduce
    	
    	start = Instant.now();
    	Configuration conf3 = new Configuration();
    	conf3.set("delimiter", delimiter);
    	Job job3 = Job.getInstance(conf3,"job3");
    	job3.setJarByClass(CustomerActionFilter.class);
    	job3.setMapperClass(Map1.class);
    	job3.setReducerClass(Reduce1.class);
    	job3.setMapOutputKeyClass(NullWritable.class);
    	job3.setMapOutputValueClass(Text.class);
    	job3.setOutputKeyClass(NullWritable.class);
    	job3.setOutputValueClass(Text.class);
    	job3.setNumReduceTasks(reducer);
		Path inpath3 = new Path(inputurl3);
		Path outpath3 = new Path(outputurl3);
		HdfsFileUtil.removeHfile(conf3, outputurl3);
		FileInputFormat.addInputPath(job3, inpath3);
		FileOutputFormat.setOutputPath(job3, outpath3);
		boolean f = job3.waitForCompletion(true);
        end = Instant.now();
        log.info(tablename1+"清洗第3次耗时："+Duration.between(start, end).toMillis()+"ms！");
        log.info(tablename1+"清洗完成，总耗时："+Duration.between(start1, end).toMillis()+"ms！");
        System.exit(f ? 0 : 1);
    }

	private static void configureArgs(String[] args) {
		tablename1 = args[0];
		inputurl1 +=tablename1;
		outputurl1 +=tablename1;
		log.info("参数列表1：\t tablename1="+tablename1+"\t inputurl1="+inputurl1+"\t outputurl1="+outputurl1);
		tablename2 += tablename1+"2";
		inputurl2 = outputurl1;
		outputurl2 += tablename2;
		log.info("参数列表2：\t tablename2="+tablename2+"\t inputurl2="+inputurl2+"\t outputurl2="+outputurl2);
		tablename3 += tablename1+"3";
		inputurl3 = outputurl2;
		outputurl3 += tablename3;
		log.info("参数列表3：\t tablename3="+tablename3+"\t inputurl3="+inputurl3+"\t outputurl3="+outputurl3);
	}
	
}