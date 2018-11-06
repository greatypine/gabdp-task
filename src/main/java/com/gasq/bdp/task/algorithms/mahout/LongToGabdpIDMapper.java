package com.gasq.bdp.task.algorithms.mahout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gasq.bdp.task.util.DateUtil;

/**
 * 将合并文件将推荐结果集转化为String类型
 * @author Renmian
 *
 */
public final class LongToGabdpIDMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/**
	 * 分隔符（每一行hadoop中的）
	 */
	public static final Pattern CSV_DELIMITER = Pattern.compile(",");
	public static final Pattern KV_DELIMITER = Pattern.compile("\t");
	
	private static boolean resultNeedPres = false;
	
	private static List<String> userIDList = new ArrayList<String>();
	private static List<String> itemIDList = new ArrayList<String>();
	
	private void loadFileToList(Configuration configuration, URI filePathUri, List<String> list) throws IOException {
		FSDataInputStream fin = null;
		BufferedReader reader = null;
		FileSystem fs = FileSystem.get(filePathUri, configuration);
		fin = fs.open(new Path(filePathUri.getPath()));
		reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		String line = null;
		int lineNo = 0;
		while (null != (line = reader.readLine())) {
			lineNo++;
			String[] result = CSV_DELIMITER.split(line);
			if(result.length != 2) {
				continue;
			}
			if(Integer.parseInt(result[1]) == lineNo) {
				list.add(result[0]);
			} else if(Integer.parseInt(result[1]) < lineNo) {
				list.set(Integer.parseInt(result[1]), result[0]);
			} else {
				//nothing to do
				//输入的值大于行号
			}
		}
		IOUtils.closeStream(reader);
		IOUtils.closeStream(fin);
	}
	
	/**
	 * 数据类型转换
	 * 将数据装在list中
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {	
		Configuration configuration = context.getConfiguration();
		URI[] uris = context.getCacheFiles();
		if (uris.length != 2) {
			throw new IOException("参数格式不符合规范，第一个是UserID Mapping文件，第二个是ItemID Mapping文件");
		}
		userIDList.add("NONE");
		itemIDList.add("NONE");
		loadFileToList(configuration, uris[0], userIDList);
		loadFileToList(configuration, uris[1], itemIDList);
	
		resultNeedPres = false;
	}
	
	
	/**
	 * org.apache.mahout.cf.taste.hadoop.item.AggregateAndRecommendReducer
	 * 推荐结果：
	 * userID/t[itemId1:value1,itemId2:value2,....]/tdate
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String cdate = DateUtil.today();
		String[] line = KV_DELIMITER.split(value.toString());
		if(line.length == 2 && line[1].length() > 2) {	//有效keyvalue
			String userId = userIDList.get(Integer.parseInt(line[0]));
			String recommendItems = line[1].substring(1, line[1].length() - 1); 	//取消掉中括号
			String[] items = CSV_DELIMITER.split(recommendItems);
			StringBuffer itemStr = new StringBuffer();
			for (int i = 0; i < items.length; i++) {
				String[] itemScore = items[i].split(":");
				if(itemScore.length != 2)
					continue;
				itemStr.append(itemIDList.get(Integer.parseInt(itemScore[0])));
				if(resultNeedPres) {
					itemStr.append(":" + itemScore[1]);
				}
				if(i < items.length - 1)
					itemStr.append(",");
			}
//			String rkey = String.join("\t", cdate,userId);
			String outVal = String.join("\t", itemStr.toString(), cdate);
			context.write(new Text(userId), new Text(outVal));
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		userIDList.clear();
		itemIDList.clear();
	}

}
