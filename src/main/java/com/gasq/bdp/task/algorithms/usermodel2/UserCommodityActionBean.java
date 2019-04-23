package com.gasq.bdp.task.algorithms.usermodel2;

import java.io.Serializable;

/**
 * 用户商品行为模型
 * @author RenMian
 *
 */
public class UserCommodityActionBean implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 * @author RenMian
	 *
	 */

//	private String action;
	private String request_date;
	private int diffday;
	private String customer_id;
	private String tag_level4_id;
	private long score;
	public String getRequest_date() {
		return request_date;
	}
	public void setRequest_date(String request_date) {
		this.request_date = request_date;
	}
	public int getDiffday() {
		return diffday;
	}
	public void setDiffday(int diffday) {
		this.diffday = diffday;
	}
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
	public long getScore() {
		return score;
	}
	public void setScore(long score) {
		this.score = score;
	}
//	public String getAction() {
//		return action;
//	}
//	public void setAction(String action) {
//		this.action = action;
//	}
}
