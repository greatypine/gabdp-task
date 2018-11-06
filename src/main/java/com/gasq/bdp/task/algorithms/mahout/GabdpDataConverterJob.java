package com.gasq.bdp.task.algorithms.mahout;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将id的String转化为支持mahout推荐算法支持的Long类型
 * 将其输出作为推荐算法的输入
 */
public class GabdpDataConverterJob extends GabdpBaseMRJob {
	
	Path userIdCachePath;
	Path itemIdCachePath;

	@Override
	public int run(String[] args) throws Exception {
		
		addInputOption();
	    addOutputOption();

	    Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		/**
		 * 将id的String转化为支持mahout推荐算法支持的Long类型
		 * 将其输出作为推荐算法的输入
		 */
		// MR任务构造
		Configuration conf = getConf();
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		Job idToLongJob = Job.getInstance(conf, "GabdpDataConverterJob.GabdpIDToLongMapper");
		idToLongJob.setNumReduceTasks(0);
		idToLongJob.setJarByClass(GabdpIDToLongMapper.class);
		idToLongJob.setMapperClass(GabdpIDToLongMapper.class);
		idToLongJob.setMapOutputKeyClass(Text.class);
		idToLongJob.setMapOutputValueClass(NullWritable.class);
		//清空删除输出路径(temp/out)
		clearPath(getOutputPath().toString(), conf);
		FileInputFormat.addInputPath(idToLongJob, getInputPath());
		FileOutputFormat.setOutputPath(idToLongJob, getOutputPath());
		// 设置缓存目录
		userIdCachePath = getIDCachePath(USERID_CACHE_OPTION);
		itemIdCachePath = getIDCachePath(ITEMID_CACHE_OPTION);
		validPath(userIdCachePath.toString(), conf);
		validPath(itemIdCachePath.toString(), conf);
		idToLongJob.addCacheFile(new URI(userIdCachePath.toString()));
		idToLongJob.addCacheFile(new URI(itemIdCachePath.toString()));

		boolean isSuccess = idToLongJob.waitForCompletion(true);
		
		return isSuccess ? JOB_RESULT_SUCCESS : JOB_RESULT_FAILURE;
	}
	
	public static void main(String[] args) throws Exception {
		//"--input /user/algorithm/userCommodityRecommendation/f_SumAlphCustomerAction --output /user/renm/test/id2long"
		ToolRunner.run(new Configuration(), new GabdpDataConverterJob(), args);
	}

}
