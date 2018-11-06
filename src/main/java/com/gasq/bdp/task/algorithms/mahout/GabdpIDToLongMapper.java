package com.gasq.bdp.task.algorithms.mahout;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gasq.bdp.task.util.HdfsFileUtil;

/**
 * format用户评分矩阵 将输入的文件转为符合RecommendJob推荐的规范：userId,itemId,score
 * userId和itemId是Long类型 score是Double类型
 * 
 * @author Renmian
 *
 */
public final class GabdpIDToLongMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");	//兼容hive表的分隔符

	private static Configuration configuration;
	private Text keyText = new Text();

	private static String userIdMapFilePath = null;
	private static String itemIdMapFilePath = null;
	//讲究放入的顺序
	private static Map<String, Long> userIDMap = new LinkedHashMap<String, Long>();
	private static Map<String, Long> itemIDMap = new LinkedHashMap<String, Long>();

	/**
	 * userId itemId对应转换关系
	 * 
	 * @throws IOException
	 */

	@Override
	protected void setup(Context context) throws IOException {
		configuration = context.getConfiguration();
		URI[] uris = context.getCacheFiles();
		if (uris.length != 2) {
			throw new IOException("参数格式不符合规范，第一个是UserID Mapping文件，第二个是ItemID Mapping文件");
		}
		userIdMapFilePath = uris[0].getPath();
		itemIdMapFilePath = uris[1].getPath();
	}

	/**
	 * map文件第一个做映射输出，同时将mapping文件写文件
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 输出
		String[] vals = DELIMITER.split(value.toString());
		if(vals.length != 3) {	//格式判断
			return;
		}
		long userId  = 1l;
		long itemId = 1l;
		if(userIDMap.containsKey(vals[0])) {
			userId = userIDMap.get(vals[0]);
		} else {
			userId = userIDMap.size() + 1;
			userIDMap.put(vals[0], userId);
		}
		if(itemIDMap.containsKey(vals[1])) {
			itemId = itemIDMap.get(vals[1]);
		} else {
			itemId = itemIDMap.size() + 1;
			itemIDMap.put(vals[1], itemId);		
		}
		keyText.set(String.join(",", ""+userId, ""+itemId, vals[2]));
		context.write(keyText, NullWritable.get());
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		HdfsFileUtil.writeLines(configuration, userIdMapFilePath, userIDMap, ",");
		HdfsFileUtil.writeLines(configuration, itemIdMapFilePath, itemIDMap, ",");
		userIDMap.clear();
		itemIDMap.clear();
	}

}
