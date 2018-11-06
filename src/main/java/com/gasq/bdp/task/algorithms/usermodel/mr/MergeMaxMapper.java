package com.gasq.bdp.task.algorithms.usermodel.mr;
import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeMaxMapper extends MapReduceBase implements Mapper<Text ,IntWritable,Text,IntWritable>{
	private Logger log = LoggerFactory.getLogger(MergeMaxMapper.class);
//	private Map<Text,ArrayList<IntWritable>> outMap= new HashMap<Text,ArrayList<IntWritable>>();
	@Override
	public void configure(JobConf conf){
		log.info("This is the begin of MergeMaxMapper");
	}
	
	@Override
	public void map(Text key, IntWritable value,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		log.info(key.toString()+"_MergeMaxMapper"+"-->"+value.get());
		out.collect(new Text(key.toString()+"_MergeMaxMapper"), value);
		
	}
	
	@Override
	public void close(){
		log.info("this is the end of MergeMaxMapper");
	}
}
