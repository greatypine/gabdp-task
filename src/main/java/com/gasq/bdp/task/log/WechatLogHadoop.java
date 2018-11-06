/**
 * 
 */
package com.gasq.bdp.task.log;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.util.HdfsFileUtil;

/**
 * @author Ju_weigang
 * @时间 2018年9月7日上午9:37:12
 * @项目路径 com.gasq.bdp.task.log
 * @描述 
 */
public class WechatLogHadoop {
	
	static Logger logger = LoggerFactory.getLogger(WechatLogHadoop.class);
	
	public static void main(String[] args) {
		try {
			args = new String[2];
//			String inputpath = "hdfs://10.10.20.19:8020/user/wx_log_input/20180905.log";
			String inputpath = "D:\\logs\\20180905.log";
			String output =  "hdfs://10.10.20.19:8020/user/juwg/WechatLogHadoop";
			args[0] = inputpath;
			args[1] = output;
			run(args);
		} catch (Exception e) {
			logger.error("处理微信日志出错："+e.getMessage(),e);
		}
	}
	
	public static void run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Instant start = Instant.now();
		Configuration conf = new Configuration();
		conf.set("flag", "\t");
        Job job = Job.getInstance(conf,"WechatLogHadoop");
        job.setJarByClass(WechatLogHadoop.class);
        job.setMapperClass(WechatLogHadoopMapper.class);
        job.setInputFormatClass(TextInputFormat.class); //输入文本格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        HdfsFileUtil.removeHfile(conf,args[1]);
        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean f = job.waitForCompletion(true);
        logger.info("WechatLogHadoop运行完成--------------总用时："+Duration.between(start, Instant.now()).getSeconds()+"秒！");
        System.exit(f ? 0 : 1);
	}

	public static class WechatLogHadoopMapper extends Mapper<Writable,Text,NullWritable,Text> {
		@Override
		protected void map(Writable key, Text value, Mapper<Writable, Text, NullWritable, Text>.Context context)throws IOException, InterruptedException {
			String f = value.toString();
			String wXlog = HandleWXlog.handleWXlog(f);
			if(StringUtils.isNotBlank(wXlog)) {
				context.write(NullWritable.get(), new Text(wXlog));
			}
		}
		
	}
	
}
