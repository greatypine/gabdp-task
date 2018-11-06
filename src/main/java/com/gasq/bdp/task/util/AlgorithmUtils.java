package com.gasq.bdp.task.util;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gasq.bdp.task.db.HiveService;

public class AlgorithmUtils {
	
	private static Logger log = LoggerFactory.getLogger(AlgorithmUtils.class);
	
	public Map<String, Double> calculationProjectsRecommend_1(String tablename) throws Exception {
		Map<String,Double> resmap = new HashMap<String,Double>();
		double mininterval = 0.0;
		double maxinterval = 0.0;
		try {
			int index = 0;
			double sum= 0.0;
			ResultSet res = HiveService.getResultSet(null, "select a.score from "+tablename+" a where a.score>0");
			ResultSet countAndSum = HiveService.getResultSet(null, "select count(1),sum(a.score) from "+tablename+" a where a.score>0");
			while (countAndSum.next()) {
				index = countAndSum.getInt(1);
				sum = countAndSum.getDouble(2);
			}
			log.info("---------------------计算完成(a_1+a_2+⋯+a_n)-->"+sum+"\t n--->"+index);
			
			double mv = new BigDecimal(sum).divide(new BigDecimal(index),6,BigDecimal.ROUND_HALF_UP).doubleValue();
			log.info("---------------------计算完成 μ=(a_1+a_2+⋯+a_n)/n完成-->"+mv);
			
			BigDecimal totalpow = BigDecimal.ZERO;
			
			while (res.next()) {
				int cn = res.getInt(1);
				totalpow = totalpow.add(new BigDecimal(Math.pow(cn-mv,2))); 
			}
			log.info("---------------------计算完成(a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2-->"+totalpow.doubleValue());
			
			double sqrtv = new BigDecimal(Math.sqrt(totalpow.divide(new BigDecimal(index),2,BigDecimal.ROUND_HALF_UP).doubleValue())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			log.info("---------------------计算完成δ=√(((a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2)/n)-->"+sqrtv);
			
			log.info("---------------------开始计算数据区间------------------------------");
			
			mininterval = new BigDecimal(mv-3*sqrtv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			maxinterval = new BigDecimal(3*sqrtv+mv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			
			log.info("---------------------计算数据区间完成(μ_1-〖3δ〗_1,3δ_1+μ_1)--->("+mininterval+","+maxinterval+")");
			
		} catch (Exception e) {
			throw new Exception("计算商品推荐有小区间值出错！",e);
		}finally {
			HiveService.closeConn();
		}
		resmap.put("mininterval", mininterval);
		resmap.put("maxinterval", maxinterval);
		return resmap;
	}
	
	public static Map<String, Double> calculationProjectsRecommend(int index,BigDecimal sum,List<Integer> cns) throws Exception {
		Map<String,Double> resmap = new HashMap<String,Double>();
		double mininterval = 0.0;
		double maxinterval = 0.0;
		try {
			log.info("---------------------计算完成(a_1+a_2+⋯+a_n)-->"+sum.intValue()+"\t n--->"+index);
			
			double mv = sum.divide(new BigDecimal(index),6,BigDecimal.ROUND_HALF_UP).doubleValue();
			log.info("---------------------计算完成 μ=(a_1+a_2+⋯+a_n)/n完成-->"+mv);
			
			BigDecimal totalpow = BigDecimal.ZERO;
			for (Integer cn : cns) {
				totalpow = totalpow.add(new BigDecimal(Math.pow(cn-mv,2))); 
			}
			log.info("---------------------计算完成(a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2-->"+totalpow.doubleValue());
			
			double sqrtv = new BigDecimal(Math.sqrt(totalpow.divide(new BigDecimal(cns.size()),2,BigDecimal.ROUND_HALF_UP).doubleValue())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			log.info("---------------------计算完成δ=√(((a_1-μ)^2+(a_2-μ)^2+⋯+(a_n-μ)^2)/n)-->"+sqrtv);
			
			log.info("---------------------开始计算数据区间------------------------------");
			
			mininterval = new BigDecimal(mv-3*sqrtv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			maxinterval = new BigDecimal(3*sqrtv+mv).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			
			log.info("---------------------计算数据区间完成(μ_1-〖3δ〗_1,3δ_1+μ_1)--->("+mininterval+","+maxinterval+")");
			
		} catch (Exception e) {
			throw new Exception("计算商品推荐有小区间值出错！",e);
		}
		resmap.put("mininterval", mininterval);
		resmap.put("maxinterval", maxinterval);
		return resmap;
	}

}
