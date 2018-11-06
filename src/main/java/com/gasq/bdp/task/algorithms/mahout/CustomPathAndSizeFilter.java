package com.gasq.bdp.task.algorithms.mahout;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 自定义文件过滤器 文件名需要满足指定的正则表达式，并且满足最小文件大小要求
 * 
 * @author Renmian
 *
 */
public class CustomPathAndSizeFilter extends Configured implements PathFilter {

	private Configuration configuration;
	private Pattern filePattern;
	private long filterSize;
	private FileSystem fileSystem;
	
	public final static String FILTER_FILE_NMAE = "filter.name";
	public final static String FILTER_FILE_MIN_SIZE = "filter.min.size";

	@Override
	public boolean accept(Path path) {
		boolean isFileAcceptable = true;
		try {
			if (fileSystem.isDirectory(path)) {
				return true;
			}
			if (filePattern != null) {
				Matcher m = filePattern.matcher(path.toString());
				isFileAcceptable = m.matches();
			}
			if (filterSize > 0) {
				long actualFileSize = fileSystem.getFileStatus(path).getLen();
				if (actualFileSize > this.filterSize) {
					isFileAcceptable &= true;
				} else {
					isFileAcceptable = false;
				}
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isFileAcceptable;
	}

	@Override
	public void setConf(Configuration conf) {
		this.configuration = conf;
		if (this.configuration != null) {
			String filterRegex = this.configuration.get(FILTER_FILE_NMAE);
			if (filterRegex != null) {
				this.filePattern = Pattern.compile(filterRegex);
			}

			String filterSizeString = this.configuration.get(FILTER_FILE_MIN_SIZE);
			if (filterSizeString != null) {
				this.filterSize = Long.parseLong(filterSizeString);
			}
			try {
				this.fileSystem = FileSystem.get(this.configuration);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
