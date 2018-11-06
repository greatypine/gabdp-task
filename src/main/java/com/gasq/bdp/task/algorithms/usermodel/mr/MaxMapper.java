package com.gasq.bdp.task.algorithms.usermodel.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.util.DelimiterType;


public class MaxMapper extends MapReduceBase implements Mapper<Writable,Text,Text,Text>{
	private Logger log = LoggerFactory.getLogger(MaxMapper.class);
	private String delimiter="";
	@Override
	public void configure(JobConf conf){
		delimiter=DelimiterType.getValueByName(conf.get("delimiter"));
		log.info("delimiter:"+delimiter);
		log.info("This is the begin of MaxMapper");
	}
	
	@Override
	public void map(Writable key, Text value,OutputCollector<Text,Text> out, Reporter reporter)throws IOException {
		out.collect(new Text(), value);
		
	}
	public void close(){
		log.info("This is the end of MaxMapper");
	}
}
