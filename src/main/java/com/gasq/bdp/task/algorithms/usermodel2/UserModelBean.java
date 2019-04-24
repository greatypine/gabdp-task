package com.gasq.bdp.task.algorithms.usermodel2;

import java.io.Serializable;

public class UserModelBean implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5974011142759973555L;
//	private String action;
	private String customer_id;
	private String tag_level4_id;
	private double score;
	
//	public String getAction() {
//		return action;
//	}
//	public void setAction(String action) {
//		this.action = action;
//	}
	public String getCustomer_id() {
		return customer_id;
	}
	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}
	public String getTag_level4_id() {
		return tag_level4_id;
	}
	public void setTag_level4_id(String tag_level4_id) {
		this.tag_level4_id = tag_level4_id;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	@Override
	public String toString() {
		return String.join(ExtractUserModel.DELIMER, customer_id, tag_level4_id, String.valueOf(score));
	}
	
	
}
