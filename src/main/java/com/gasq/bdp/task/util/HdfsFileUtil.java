package com.gasq.bdp.task.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 操作hdfs文件
 * 
 * @author Renmian
 *
 */
public final class HdfsFileUtil {

	public static final String LINE_SEPARATOR;
	static {
		// avoid security issues
		StringBuilderWriter buf = new StringBuilderWriter(4);
		PrintWriter out = new PrintWriter(buf);
		out.println();
		LINE_SEPARATOR = buf.toString();
		out.close();
	}

	/**
	 * 检查文件是否存在
	 * 
	 * @param path
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static boolean isExist(String path, Configuration conf) throws IOException {
		FileSystem fs = null;
		Boolean isexists = null;
		try {
			Path p = new Path(path);
			fs = p.getFileSystem(conf);
			isexists = fs.exists(p);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fs.close();
		}

		return isexists;
	}

	/**
	 * 删除文件
	 * @param conf
	 * @param ioPath
	 * @throws IOException
	 */
	public static void deleteHfile(Configuration conf, String ioPath) throws IOException {
		FileSystem fileSystem = null;
		try {
			Path path = new Path(ioPath);// 取第1个表示输出目录参数（第0个参数是输入目录）  
			fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
			fileSystem.delete(path, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void removeHfile(Configuration conf,String ppath) {
		FileSystem fileSystem = null;
		try {
			// 判断output文件夹是否存在，如果存在则删除  
			Path path = new Path(ppath);// 取第1个表示输出目录参数（第0个参数是输入目录）  
			fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
			if (fileSystem.exists(path)) {  
				fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
			} 
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(fileSystem!=null)fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void touch(Configuration conf, String file) {
		try {
			FSDataOutputStream os = openOutputStream(conf, file, false);
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static FSDataOutputStream openOutputStream(Configuration conf, String file, boolean append)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(file);
		FSDataOutputStream os = null;
		if (!fs.exists(path) || !append) {
			os = fs.create(path, true);
		} else {
			os = fs.append(path);
		}
		return os;
	}

	/**
	 * 按行写文件
	 * @param conf
	 * @param file
	 * @param lines
	 * @param kvSplit
	 * @throws IOException
	 */
	public static void writeLines(Configuration conf, String file, Map<?, ?> lines, String kvSplit) throws IOException {
		writeLines(conf, file, null, lines, null, kvSplit, false);
	}

	public static void writeLines(Configuration conf, String file, String encoding, Map<?, ?> lines, String lineEnding,
			String kvSplit, boolean append) throws IOException {
		FSDataOutputStream os = null;
		try {
			os = openOutputStream(conf, file, append);
			if (lines == null) {
				return;
			}
			if (lineEnding == null) {
				lineEnding = LINE_SEPARATOR;
			}
			Charset cs = Charsets.toCharset(encoding);
			for (Entry<?, ?> entry : lines.entrySet()) {
				if (entry != null) {
					os.write((entry.getKey().toString() + kvSplit + entry.getValue().toString()).getBytes(cs));
				}
				os.write(lineEnding.getBytes(cs));
			}
			os.flush();
		} finally {
			if (os != null)
				os.close();
		}
	}
}
