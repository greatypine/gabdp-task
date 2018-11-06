package com.gasq.bdp.task.algorithms.mahout;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

/**
 * 
 * @author Renmian
 *
 */
public class GabdpBasedItemRecommendJob extends GabdpBaseMRJob {

	public static final String NEED_PRES = "needPres";

	private static final String RECOMMEND_INPUT_PATH = "rinput";
	private static final String RECOMMEND_OUTPUT_PATH = "routput";
	private static final String RECOMMEND_TEMP_PATH = "rtemp";

	static final int DEFAULT_NUM_RECOMMENDATIONS = 10;
	static final int DEFAULT_MAX_PREFS_PER_USER_CONSIDERED = 10;
	static final int DEFAULT_MIN_PREFS_PER_USER = 1;
	static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
	static final int DEFAULT_MAX_PREFS = 500;

	private boolean isNeedPres;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GabdpBasedItemRecommendJob(), args);
	}

	private String[] getRecommendJobArgs(Path recommendInputPath, Path recommendOutPath, Path recommendTempPath) {
		List<String> args = new ArrayList<String>();
		if (StringUtils.isNotBlank(recommendInputPath.toString())) {
			args.add("--input");
			args.add(recommendInputPath.toString());
		}
		if (StringUtils.isNotBlank(getInputPath().toString())) {
			args.add("--output");
			args.add(recommendOutPath.toString());
		}
		if (StringUtils.isNotBlank(recommendTempPath.toString())) {
			args.add("--tempDir");
			args.add(recommendTempPath.toString());
		}
		if (StringUtils.isNotBlank(getOption("similarityClassname"))) {
			args.add("--similarityClassname");
			args.add(getOption("similarityClassname"));
		}
		if (StringUtils.isNotBlank(getOption("numRecommendations"))) {
			args.add("--numRecommendations");
			args.add(getOption("numRecommendations"));
		}
		if (StringUtils.isNotBlank(getOption("usersFile"))) {
			args.add("--usersFile");
			args.add(getOption("usersFile"));
		}
		if (StringUtils.isNotBlank(getOption("itemsFile"))) {
			args.add("--itemsFile");
			args.add(getOption("itemsFile"));
		}
		if (StringUtils.isNotBlank(getOption("filterFile"))) {
			args.add("--filterFile");
			args.add(getOption("filterFile"));
		}
		if (StringUtils.isNotBlank(getOption("userItemFile"))) {
			args.add("--userItemFile");
			args.add(getOption("userItemFile"));
		}
		if (StringUtils.isNotBlank(getOption("booleanData"))) {
			args.add("--booleanData");
			args.add(getOption("booleanData"));
		}
		if (StringUtils.isNotBlank(getOption("maxPrefsPerUser"))) {
			args.add("--maxPrefsPerUser");
			args.add(getOption("maxPrefsPerUser"));
		}
		if (StringUtils.isNotBlank(getOption("minPrefsPerUser"))) {
			args.add("--minPrefsPerUser");
			args.add(getOption("minPrefsPerUser"));
		}
		if (StringUtils.isNotBlank(getOption("maxSimilaritiesPerItem"))) {
			args.add("--maxSimilaritiesPerItem");
			args.add(getOption("maxSimilaritiesPerItem"));
		}
		if (StringUtils.isNotBlank(getOption("maxPrefsInItemSimilarity"))) {
			args.add("--maxPrefsInItemSimilarity");
			args.add(getOption("maxPrefsInItemSimilarity"));
		}
		if (StringUtils.isNotBlank(getOption("threshold"))) {
			args.add("--threshold");
			args.add(getOption("threshold"));
		}
		if (StringUtils.isNotBlank(getOption("outputPathForSimilarityMatrix"))) {
			args.add("--outputPathForSimilarityMatrix");
			args.add(getOption("outputPathForSimilarityMatrix"));
		}
		if (StringUtils.isNotBlank(getOption("randomSeed"))) {
			args.add("--randomSeed");
			args.add(getOption("randomSeed"));
		}
		if (StringUtils.isNotBlank(getOption("sequencefileOutput"))) {
			args.add("--sequencefileOutput");
			args.add(getOption("sequencefileOutput"));
		}
		return args.toArray(new String[0]);
	}

	@Override
	public int run(String[] args) throws Exception {
		// Copy From RecommendJob Arguments
		addInputOption();
		addOutputOption();

		addOption("numRecommendations", "n", "Number of recommendations per user",
				String.valueOf(DEFAULT_NUM_RECOMMENDATIONS));
		addOption("usersFile", null, "File of users to recommend for", null);
		addOption("itemsFile", null, "File of items to recommend for", null);
		addOption("filterFile", "f",
				"File containing comma-separated userID,itemID pairs. Used to exclude the item from "
						+ "the recommendations for that user (optional)",
				null);
		addOption("userItemFile", "uif",
				"File containing comma-separated userID,itemID pairs (optional). "
						+ "Used to include only these items into recommendations. "
						+ "Cannot be used together with usersFile or itemsFile",
				null);
		addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
		addOption("maxPrefsPerUser", "mxp",
				"Maximum number of preferences considered per user in final recommendation phase",
				String.valueOf(DEFAULT_MAX_PREFS_PER_USER_CONSIDERED));
		addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this in the similarity computation "
				+ "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		addOption("maxSimilaritiesPerItem", "m", "Maximum number of similarities considered per item ",
				String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ITEM));
		addOption("maxPrefsInItemSimilarity", "mpiis",
				"max number of preferences to consider per user or item in the "
						+ "item similarity computation phase, users or items with more preferences will be sampled down (default: "
						+ DEFAULT_MAX_PREFS + ')',
				String.valueOf(DEFAULT_MAX_PREFS));
		addOption("similarityClassname", "s", "Name of distributed similarity measures class to instantiate, "
				+ "alternatively use one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')',
				true);
		addOption("threshold", "tr", "discard item pairs with a similarity value below this", false);
		addOption("outputPathForSimilarityMatrix", "opfsm", "write the item similarity matrix to this path (optional)",
				false);
		addOption("randomSeed", null, "use this seed for sampling", false);
		addFlag("sequencefileOutput", null, "write the output into a SequenceFile instead of a text file");
		// --end
		// result output need preference
		addOption(NEED_PRES, "np", "Recommend Output Result Need Preference score or Not", false);

		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return JOB_RESULT_FAILURE;
		}
		// id2LongJob
		// 第一步：模型String id转Long类型
		List<String> step1Args = new ArrayList<String>();
		step1Args.add("--input");
		step1Args.add(getInputPath().toString());
		step1Args.add("--output");
		step1Args.add(getTempPath(RECOMMEND_INPUT_PATH).toString());
		step1Args.add("--tempDir");
		step1Args.add(getTempPath().toString());
		int jobResult = ToolRunner.run(getConf(), new GabdpDataConverterJob(), step1Args.toArray(new String[0]));
		if (jobResult == JOB_RESULT_FAILURE) {
			throw new GabdpMRJobException("Step1:GabdpIdToLong Job<GabdpDataConverterJob> failure!");
		}
		/**
		 * 第二步推荐算法  推荐算法RecommendJOB
		 */
		isNeedPres = Boolean.valueOf(getOption(NEED_PRES, "false"));
		Path recommendInputPath = getTempPath(RECOMMEND_INPUT_PATH);
		Path recommendOutPath = getTempPath(RECOMMEND_OUTPUT_PATH);
		Path recommendTempPath = getTempPath(RECOMMEND_TEMP_PATH);
		// 清空删除输出路径(temp/out)
		clearPath(recommendTempPath.toString(), getConf());
		clearPath(recommendOutPath.toString(), getConf());

		jobResult = ToolRunner.run(getConf(), new RecommenderJob(),
				getRecommendJobArgs(recommendInputPath, recommendOutPath, recommendTempPath));
		if (jobResult == JOB_RESULT_FAILURE) {
			throw new GabdpMRJobException("Step2:RecommenderJob Job<RecommenderJob> failure!");
		}

		Configuration conf = getConf();
		conf.set(CustomPathAndSizeFilter.FILTER_FILE_MIN_SIZE, "1"); // 最小文件大小定义
		conf.set(CustomPathAndSizeFilter.FILTER_FILE_NMAE, ".*(part-r-)*"); // 满足part-r-*的文件
		conf.setBoolean(NEED_PRES, isNeedPres); // 是否需要输出推荐评分
		clearPath(getOutputPath().toString(), getConf());
		Job long2IdJob = Job.getInstance(conf, "GabdpBasedItemRecommendJob.LongToGabdpIDMapper");
		long2IdJob.setNumReduceTasks(0);
		long2IdJob.setJarByClass(LongToGabdpIDMapper.class);
		long2IdJob.setMapperClass(LongToGabdpIDMapper.class);
		long2IdJob.setMapOutputKeyClass(Text.class);
		long2IdJob.setMapOutputValueClass(NullWritable.class);

		long2IdJob.setInputFormatClass(CustomCombineFileInputFormat.class); // 设置文件格式
		FileInputFormat.setInputPathFilter(long2IdJob, CustomPathAndSizeFilter.class); // 设置过滤器

		FileInputFormat.addInputPath(long2IdJob, recommendOutPath);
		FileOutputFormat.setOutputPath(long2IdJob, getOutputPath());

		long2IdJob.addCacheFile(new URI(getIDCachePath(USERID_CACHE_OPTION).toString()));
		long2IdJob.addCacheFile(new URI(getIDCachePath(ITEMID_CACHE_OPTION).toString()));
		boolean isSuccess = long2IdJob.waitForCompletion(true);
		return isSuccess ? JOB_RESULT_SUCCESS : JOB_RESULT_FAILURE;
	}
}
