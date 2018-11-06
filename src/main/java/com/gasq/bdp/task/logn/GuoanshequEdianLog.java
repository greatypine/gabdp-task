package com.gasq.bdp.task.logn;

import java.io.Serializable;

public class GuoanshequEdianLog implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7520363262986880738L;
	/**
	 * 表mapping的对象   日志数据表
	 */
	public static final String TBL_NAME = "t_log";
	
	private String TERMINATED = "\t";
	private String request_time;
	private String ip;
	private String store_id;
	private String customer_id;
	private String apptypeplatform;
	private String app_com;
	private String task;
	private String body;
	private String mac_address;
	private String request_date;
	
	@SuppressWarnings("unused")
	private GuoanshequEdianLog() {}
	
	public GuoanshequEdianLog(String TERMINATED) {
		this.TERMINATED = TERMINATED;
	}
	
	public String getRequest_time() {
		return request_time;
	}
	public void setRequest_time(String request_time) {
		this.request_time = request_time;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getStore_id() {
		return store_id;
	}
	public void setStore_id(String store_id) {
		this.store_id = store_id;
	}
	public String getCustomer_id() {
		return customer_id;
	}
	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}
	public String getApptypeplatform() {
		return apptypeplatform;
	}
	public void setApptypeplatform(String apptypeplatform) {
		this.apptypeplatform = apptypeplatform;
	}
	public String getApp_com() {
		return app_com;
	}
	public void setApp_com(String app_com) {
		this.app_com = app_com;
	}
	public String getTask() {
		return task;
	}
	public void setTask(String task) {
		this.task = task;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getMac_address() {
		return mac_address;
	}
	public void setMac_address(String mac_address) {
		this.mac_address = mac_address;
	}
	public String getRequest_date() {
		return request_date;
	}
	public void setRequest_date(String request_date) {
		this.request_date = request_date;
	}
	/**
	 * 根据hive表结构来输出字段
	 */
	@Override
	public String toString() {
		return String.join(TERMINATED, request_time, ip, store_id, customer_id, apptypeplatform, app_com, task,
				body, mac_address, request_date);
	}
	
	/**
	 * 和表列顺序一致
	 * @return
	 */
	public String[] getColumns() {
		String[] columns = {request_time, ip, store_id, customer_id, apptypeplatform, app_com, task,
				body, mac_address, request_date};
		return columns;
	}
	
	/**
	 * 获取创建表的sparksql
	 * @return
	 * @deprecated spark sql 无法运行
	 */
	public String getCreateOuterTblHql(String hiveWarehousePath) {
		return "CREATE EXTERNAL TABLE IF NOT EXISTS " + TBL_NAME
				+ " (request_time string,ip string,store_id string,customer_id string,apptypeplatform string,app_com string,task string,body string,mac_address string) PARTITIONED BY (request_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
				+ TERMINATED + "' STORED AS TEXTFILE LOCATION '" + hiveWarehousePath + TBL_NAME + "'";
	}
	
	public String loadDataToTbl(String dataPath, String date) {
		return "load data inpath '" + dataPath + "' OVERWRITE into table " + TBL_NAME + " PARTITION (request_date='" + date + "')";
	}
}
