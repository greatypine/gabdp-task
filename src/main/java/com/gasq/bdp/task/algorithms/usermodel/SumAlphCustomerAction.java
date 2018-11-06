package com.gasq.bdp.task.algorithms.usermodel;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SumAlphCustomerAction {
    public static class MapReduce extends Mapper<Writable, Text, Text, Text> {
        // 实现map函数
        public void map(Writable key,Text value, Context context)throws IOException, InterruptedException {
        	 // 将输入的纯文本文件的数据转化成String
        	if(key!=null && value!=null) {
        		String line = value.toString();
        		if(StringUtils.isNotBlank(line)) {
        			String[] vs = line.split("\t");
        			if(vs.length==3) {
		            	// 将输入的数据首先按行进行分割
		            	String customer_id = vs[0];
		            	String tag_level4_id = vs[1];
		            	String alph = vs[2];
		            	String newkey =  String.join("\t",customer_id,tag_level4_id);
		            	String vv = alph;
		            	context.write(new Text(newkey), new Text(vv));
        			}
        		}
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
        	if(key!=null && !"".equals(key.toString())) {
        		Double d = 0.0;
        		for (Text value:values) {
    				if(value!=null) {
    					d += Double.parseDouble(value.toString());
    				}
            	}
        		//输出
        		context.write(key, new Text(d.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
    	System.out.println("SumAlphCustomerAction传入参数：参数1=："+args[0]+"\t"+args[1]+"\t"+args[2]+"\t"+args[3]);
    	Instant start = Instant.now();
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"SumAlphCustomerAction");
        job.setNumReduceTasks(1);
        job.setJarByClass(SumAlphCustomerAction.class);
        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(MapReduce.class);
//        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);
     // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(args[3]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
        if (fileSystem.exists(path)) {
        	fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
        }
        // 设置输入和输出目录
//        Path[] paths = {new Path("/user/algorithm/test/f_customer_req_action"), new Path("/user/algorithm/test/f_customer_add_action"),new Path("/user/algorithm/test/f_customer_suorder_action")};
//        FileInputFormat.setInputPaths(job, paths);
//        FileOutputFormat.setOutputPath(job, new Path("/user/algorithm/test/f_SumAlphCustomerAction"));
        // 设置输入和输出目录
        Path[] paths = {new Path(args[0]), new Path(args[1]),new Path(args[2])};  
        FileInputFormat.setInputPaths(job,paths);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        boolean f = job.waitForCompletion(true);
        Instant end = Instant.now();
        System.out.println("SumAlphCustomerAction计算用时："+Duration.between(start, end).toMillis()+"ms！");
        System.exit(f ? 0 : 1);
    }
}