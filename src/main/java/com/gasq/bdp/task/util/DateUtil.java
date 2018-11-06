package com.gasq.bdp.task.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class DateUtil {
	
	public final static String DEFAULT_DATE_FORMATTER = "yyyy-MM-dd";
	public final static String DEFAULT_TIME_FORMATTER = "HH:mm:ss";
	public final static String DATE_NO_FLAG_DATE_FORMAT = "yyyyMMdd";
	
	public static boolean isValidDate(String date) {
		return isValidDateTime(date, DEFAULT_DATE_FORMATTER);
	}
	
	public static boolean isValidDateTime(String date, String dateFormatter) {
		boolean isValid = false;
		if(StringUtils.isEmpty(date))
			return isValid;
        try{  
        	DateFormat formatter = new SimpleDateFormat(dateFormatter);
        	Date d = formatter.parse(date); 
        	isValid = date.equals(formatter.format(d));
        }catch(Exception e){
//            System.out.println("格式错误！");
        	isValid = false;
        }
        return isValid;
	}
	
	public static boolean isValidTime(String time) {
		return isValidDateTime(time, DEFAULT_TIME_FORMATTER);
	}
	
	/**
	 * 获取当前日期其他的日期
	 * @param difdate 天数 正数：在当前时间之上添加的天数 ，负数反之
	 * @param format 返回格式  eg DateUtil.DATE_DEFAULT_FORMAT
	 * @return
	 */
	@SuppressWarnings("static-access")
	public static String getDiyStrDateTime(int difdate,String format) {
		Date date=new Date();//取时间  
		Calendar calendar = new GregorianCalendar();  
		calendar.setTime(date);  
		calendar.add(calendar.DATE,difdate);//把日期往后增加一天.整数往后推,负数往前移动  
		date=calendar.getTime(); //这个时间就是日期往后推一天的结果   
		SimpleDateFormat formatter = new SimpleDateFormat(format);  
		String dateString = formatter.format(date);  
		return dateString;
	}
	
	/**
	 * 
	 * 
	 * @param str
	 * @return Date
	 */
	public static String getDateStr(Date date,String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.format(date);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}
	
	/**
	 * 
	 * 
	 * @param str
	 * @return Date
	 */
	public static String getDateStr(Date date) {
		return getDateStr(date, DEFAULT_DATE_FORMATTER);
	}
	
	public static String today() {
		return getDateStr(new Date());
	}
	
	public static String today(String dateFormat) {
		return getDateStr(new Date(), dateFormat);
	}
	
	@Test
	public void test() {
		String date = "2018-07-09";
		Assert.assertTrue(isValidDate(date));
		date = "2018-7-9";
		Assert.assertSame(false, isValidDate(date));
		String time = "23:58:02";
		Assert.assertTrue(isValidTime(time));
		time = "23:38";
		Assert.assertFalse(isValidTime(date));
	}

}
