/**
 * 
 */
package com.gasq.bdp.task.algorithms.usermodel;

public class CustReqActionBean{

	private String request_date;
	private int datediff;
	private String customer_id;
	private String tag_level4_id;
	private double score;
	public String getRequest_date() {
		return request_date;
	}
	public void setRequest_date(String request_date) {
		this.request_date = request_date;
	}
	public int getDatediff() {
		return datediff;
	}
	public void setDatediff(int datediff) {
		this.datediff = datediff;
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
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	
}
