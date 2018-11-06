/**
 * 
 */
package com.gasq.bdp.task.algorithms.usermodel;

import java.util.List;

public class CustomerReqActionService {
	public static Double computeScoreTable(List<CustReqActionBean> lists,Double alph) {
		double val = 0.0;
		for (CustReqActionBean custReqActionBean : lists) {
			val +=custReqActionBean.getScore()*Math.log(1+custReqActionBean.getDatediff())*alph;
		}
		return val;
	}

}
