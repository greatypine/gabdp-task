package com.gasq.bdp.task.algorithms.usermodel.mr;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.util.DelimiterType;


public   class MaxReducer extends MapReduceBase implements Reducer<Text,Text,NullWritable,Text>{
	private Logger log = LoggerFactory.getLogger(MaxReducer.class);
	private String delimiter="";
	@Override
	public void configure(JobConf conf){
		delimiter=DelimiterType.getValueByName(conf.get("delimiter"));
		log.info("delimiter:"+delimiter);
		log.info("This is the begin of the MaxReducer");
	}
	@Override
	public void reduce(Text key, Iterator<Text> values,OutputCollector<NullWritable,Text> out, Reporter reporter)throws IOException {
		int index = 0;
		BigDecimal bd= BigDecimal.ZERO;
		List<Integer> cns = new ArrayList<Integer>();
		List<String> myList = new ArrayList<String>();
		while(values.hasNext()){
			String val = values.next().toString();
			String[] beans = val.split(delimiter);
			int clicknumb = Integer.parseInt(beans[4]);
			if(clicknumb>0) {
				index++;
				bd=bd.add(new BigDecimal(clicknumb));
				cns.add(clicknumb);
				myList.add(val);
			}
		}
		log.info("---------------------计算完成(a_1+a_2+⋯+a_n)-->"+bd.intValue()+"\t n--->"+index);
		
		double mv = bd.divide(new BigDecimal(index),6,BigDecimal.ROUND_HALF_UP).doubleValue();
		log.info("---------------------计算完成 μ=(a_1+a_2+⋯+a_n)/n完成-->"+mv);
		
		BigDecimal totalpow = BigDecimal.ZERO;
		for (Integer cn : cns) {
			totalpow = totalpow.add(new BigDecimal(Math.pow(cn-mv,2))); 
		}
		log.info("---------------------计算完成(a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2-->"+totalpow.doubleValue());
		
		double sqrtv = new BigDecimal(Math.sqrt(totalpow.divide(new BigDecimal(cns.size()),2,BigDecimal.ROUND_HALF_UP).doubleValue())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		log.info("---------------------计算完成δ=√(((a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2)/n)-->"+sqrtv);
		
		log.info("---------------------开始计算数据区间------------------------------");
		
		double mininterval = new BigDecimal(mv-3*sqrtv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		double maxinterval = new BigDecimal(3*sqrtv+mv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		
		log.info("---------------------计算数据区间完成(μ_1-〖3δ〗_1,3δ_1+μ_1)--->("+mininterval+","+maxinterval+")");
		for (String text : myList) {
			String[] beans = text.split(delimiter);
			int clicknumb = Integer.parseInt(beans[4]);
			if(clicknumb>mininterval && clicknumb<maxinterval) {
				out.collect(NullWritable.get(), new Text(text));
			}
		}
	}
	
	@Override
	public void close(){
		log.info("This is the end of the MaxReducer");
	}
}