/**
 * 
 */
package com.gasq.bdp.task.mr;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gasq.bdp.task.util.DateUtil;

/**
 * @author 巨伟刚
 * @packageName com.hadoop
 * @creatTime 2017年8月30日下午4:30:46
 * @remark 
 */
public class ClearVIPUserInfoV2 {
	static Logger logger = Logger.getLogger(ClearVIPUserInfoV2.class);
	public static class CleanESLogMapper extends Mapper<Writable,Text,NullWritable,Text> {
//		ObjectMapper json = new ObjectMapper();
		public void map(Writable key,Text value, Context context) throws IOException, InterruptedException {
			String flag = context.getConfiguration().get("flag");
			String val = value.toString();
			String _result = handleStr(val,flag);
			if(StringUtils.isNotBlank(_result)) {
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
        Job job = Job.getInstance(conf,"clearVipUserInfoV2");
        job.setJarByClass(ClearVIPUserInfoV2.class);
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
        Path inpath = new Path(args[0]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        if (fileSystem.exists(inpath)) {
        	fileSystem.delete(inpath, true);// true的意思是，就算output有东西，也一带删除  
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
//			String value = "古丽欣	ca071a05ea99455aa02a3c0acbef0509	[]	15022198931	[]	2018-06-15 19:28:32	022	[]	{}	120113197210060826	2	2019-06-15 23:59:59	null	null";
//			System.out.println(handleStr(value, "\t"));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
    }
	public static String handleStr(String value, String flag) {
		if(StringUtils.isNotBlank(value)) {
			String[] split = value.split(flag);
			if(split.length==14) {
				return parseOldStrHaddler(split, flag);
			}
		}
		return null;
	}
	
	public static String parseOldStrHaddler(String[] split,String flag) {
		String name = split[0];
		if(name.endsWith("null")) split[0] = "";
//		String customerId = split[1];
		String userDefine = split[2];
		if(userDefine.endsWith("null")) split[2] = "";
		if(userDefine.equals("[]")) userDefine = null;
		else userDefine = userDefine.replaceFirst("\\[", "").replaceFirst("\\]", "");
		split[2] = userDefine;
		String phone = split[3];
		if(phone.endsWith("null")) split[3] = "";
		String hobby = split[4];
		if(hobby.equals("[]")||hobby.equals("null")) hobby = null;
		else hobby = hobby.replaceFirst("\\[", "").replaceFirst("\\]", "");
		split[4] = hobby;
//		String createdAt = split[5];
		String cityCode = split[6];
		if(cityCode.endsWith("null")) split[6] = "";
		String economyStrength = split[7];
		if(economyStrength.endsWith("null")) split[7] = "";
		String idCardInfo = split[8];
		if(idCardInfo.endsWith("null")) split[8] = "";
		String idCard = split[9];
		if(idCard.endsWith("null")) split[9] = "";
		String associatorLevel = split[10];
		if(associatorLevel.endsWith("null")) split[10] = "";
		String associatorExpiryDate = split[11];
		if(associatorExpiryDate.endsWith("null")) split[11] = "";
		String birthday = split[12];
		if(birthday.endsWith("null")) split[12] = "";
		String isFromEmployee = split[13];
		if(isFromEmployee.endsWith("null")) split[13] = "";
		if(economyStrength.equals("[]") || economyStrength.equals("null")) economyStrength = null;
		else economyStrength = economyStrength.replaceFirst("\\[", "").replaceFirst("\\]", "");
		split[7] = economyStrength;
		String sex = "";
		String address = "";
		if(StringUtils.isNotBlank(idCardInfo)&& !idCardInfo.endsWith("{}")) {
			String s = idCard.substring(0, idCard.length()-4);
			s = s+"****";
			split[8] = s;
		}else {
			idCardInfo="";
			split[8] = "";
		}
		if(StringUtils.isNotBlank(idCardInfo)) {
			String string = idCardInfo.replaceAll("\\=", ":").replaceAll(":", ":\"").replaceAll(",", "\",").replaceAll("}", "\"}");
			try {
				JSONObject jsonObject = JSON.parseObject(string);
				Set<Entry<String, Object>> entrySet = jsonObject.entrySet();
				for (Entry<String, Object> entry : entrySet) {
					if(entry.getKey().equals("sex")) {
						if(entry.getValue().equals("F")) {
							sex = "女";
						}else if(entry.getValue().equals("M")) {
							sex = "男";
						}else {
							sex = null;
						}
					}
					if(entry.getKey().equals("birthday")) {
						birthday = entry.getValue().toString();
						split[12] = birthday;
					}
					if(entry.getKey().equals("address")) {
						address = entry.getValue().toString();
					}
				}
				if(string.equals("{}")) string = null;
				else string = string.replaceFirst("\\{", "").replaceFirst("\\}", "");
				split[8] = string;
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
		String _result = StringUtils.join(split,flag);
		_result = String.join(flag, _result,sex,address);
		return _result;
	}
	
}
