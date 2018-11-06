package com.gasq.bdp.task.algorithms.usermodel;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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

public class CustomerReqAction {
	static double alph = 0.0;
    public static class MapReduce extends Mapper<Writable, Text, Text, Text> {
        // 实现map函数
        public void map(Writable key,Text value, Context context)throws IOException, InterruptedException {
        	 // 将输入的纯文本文件的数据转化成String
        	if(key!=null && value!=null) {
        		String line = value.toString();
        		if(StringUtils.isNotBlank(line)) {
        			String[] vs = line.split("");
        			if(vs.length==5) {
		            	// 将输入的数据首先按行进行分割
//		            	String request_date = vs[0];
		            	String datediff = vs[1];
		            	String customer_id = vs[2];
		            	String tag_level4_id = vs[3];
		            	String score = vs[4];
		            	String newkey =  String.join("\t",customer_id,tag_level4_id);
		            	String vv = String.join("\t", datediff,score);
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
        		List<CustReqActionBean> lists = new ArrayList<CustReqActionBean>();
    			for (Text value:values) {
    				if(value!=null) {
    					CustReqActionBean bean = new CustReqActionBean();
    					String[] strval = value.toString().split("\t");
    					bean.setDatediff(Integer.parseInt(strval[0]));
    					bean.setScore(Double.parseDouble(strval[1]));
    					lists.add(bean);
    				}
            	}
    			alph = Double.parseDouble(context.getConfiguration().get("alph").toString());
    			System.out.println("传入权重系数参数="+alph);
    			Double score = CustomerReqActionService.computeScoreTable(lists,alph);
    			//输出
    			context.write(key, new Text(score.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
    	System.out.println("CustomerReqAction传入参数：参数1=："+args[0]+"\t"+args[1]+"\t"+args[2]);
    	Instant start = Instant.now();
    	Configuration conf = new Configuration();
    	conf.set("alph", args[2]);
        Job job = Job.getInstance(conf,"CustomerReqAction");
        job.setNumReduceTasks(1);
        job.setJarByClass(CustomerReqAction.class);
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
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
        if (fileSystem.exists(path)) {  
        	fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
        }  
        // 设置输入和输出目录
//      FileInputFormat.setInputPaths(job, new Path("/user/hive/warehouse/f_customer_pro_add_new_action"));
//      FileOutputFormat.setOutputPath(job, new Path("/user/algorithm/test/f_customer_add_action"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        alph = 0.1;
        boolean f = job.waitForCompletion(true);
        Instant end = Instant.now();
        System.out.println("AprioriAdCode计算用时："+Duration.between(start, end)+"ms！");
        System.exit(f ? 0 : 1);
    }
}