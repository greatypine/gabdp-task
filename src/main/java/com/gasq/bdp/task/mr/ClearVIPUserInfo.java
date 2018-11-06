/**
 * 
 */
package com.gasq.bdp.task.mr;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.log4j.Logger;

import com.gasq.bdp.task.util.DateUtil;


/**
 * @author 巨伟刚
 * @packageName com.hadoop
 * @creatTime 2017年8月30日下午4:30:46
 * @remark 
 */
public class ClearVIPUserInfo {
	static Logger logger = Logger.getLogger(ClearVIPUserInfo.class);
	public static class CleanESLogMapper extends Mapper<Writable,Text,NullWritable,Text> {
//		ObjectMapper json = new ObjectMapper();
		public void map(Writable key,Text value, Context context) throws IOException, InterruptedException {
			String flag = context.getConfiguration().get("flag");
			String val = value.toString();
			if(StringUtils.isNotBlank(val)) {
				String[] split = val.split(flag);
				String idcard = split[5];
				String sex = split[6];
				if(idcard!=null && !"".equals(idcard)) {
					String s = idcard.substring(0, idcard.length()-4);
					s = s+"****";
					split[5] = s;
				}
				if(sex.equals("F")) {
					split[6] = "女";
				}else if(sex.equals("M")) {
					split[6] = "男";
				}else {
					split[6] = null;
				}
				String _result = StringUtils.join(split, flag);
				context.write(NullWritable.get(), new Text(_result));
			}
		}
	}
	private static void cleanESLog(String[] args) throws Exception {
		String date = DateUtil.getDiyStrDateTime(Integer.parseInt(args[2]),DateUtil.DATE_NO_FLAG_DATE_FORMAT);
		String inputfilepath = args[0]+date;
//		String outputfilepath = args[1]+date;
		Configuration conf = new Configuration();
		conf.set("flag", "\t");
        Job job = Job.getInstance(conf,"clearVipUserInfo");
        job.setJarByClass(ClearVIPUserInfo.class);
        job.setMapperClass(CleanESLogMapper.class);
        job.setInputFormatClass(TextInputFormat.class); //输入文本格式
//        job.setInputFormatClass(EsInputFormat.class);  //输入es格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
        if (fileSystem.exists(path)) {
        	fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
        }
        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(inputfilepath+".txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.setInputPaths(job, new Path(filepath));
//        FileOutputFormat.setOutputPath(job, new Path(fileoutpath));
        boolean f = job.waitForCompletion(true);
        System.exit(f ? 0 : 1);
	}
	public static void main(String[] args) throws Exception {
		try {
			cleanESLog(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
