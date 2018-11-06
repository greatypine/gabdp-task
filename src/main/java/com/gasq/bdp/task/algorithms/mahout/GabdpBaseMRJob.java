package com.gasq.bdp.task.algorithms.mahout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;

import com.gasq.bdp.task.GasqTask;
import com.gasq.bdp.task.util.HdfsFileUtil;

public class GabdpBaseMRJob extends AbstractJob implements GasqTask {

	static final String USERID_CACHE_OPTION = "userIdCache";
	static final String ITEMID_CACHE_OPTION = "itemIdCache";
	static final String ID_CACHE_PATH = "idcache";

	public final static int JOB_RESULT_SUCCESS = 0;
	public final static int JOB_RESULT_FAILURE = -1;

	protected void clearPath(String path, Configuration conf) throws IOException {
		if (HdfsFileUtil.isExist(path, conf)) {
			HdfsFileUtil.deleteHfile(conf, path);
		}
	}

	protected void validPath(String path, Configuration conf) throws IOException {
		if (HdfsFileUtil.isExist(path, conf)) {
			HdfsFileUtil.deleteHfile(conf, path);
		}
		HdfsFileUtil.touch(conf, path);
	}

	protected Path getIDCachePath(String directory) {
		return new Path(getTempPath(ID_CACHE_PATH), directory);
	}

	@Override
	public int run(String[] args) throws Exception {

		return JOB_RESULT_SUCCESS;
	}

}
