package com.gasq.bdp.task.util;


import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
  * Description:  impala工具类(连接池,非单例) 
  * @author lyy  
  * @date 2018年5月21日
 */
public class ImpalaUtils  implements Serializable{

	private static final long serialVersionUID = 1L;
	private  String jdbc_driver_name = null;
    private  String connection_url = null;
    private static final String configPath = "ansj_library.properties";
    // 定义数据库的链接
    private static Connection connection;
    // 定义sql语句的执行对象
    private static PreparedStatement pstmt;
    // 定义查询返回的结果集合
    private static ResultSet resultSet;
    
    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     */
    public  Connection getConnection() throws SQLException {
    	
    	if(connection == null){
    		try {
    			jdbc_driver_name = FileUtil.getProperties("jdbc_driver_name", configPath);
    			connection_url = FileUtil.getProperties("connection_url",configPath);
                Class.forName(jdbc_driver_name); // 注册驱动
                System.out.println("非连接池注册驱动");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            connection = DriverManager.getConnection(connection_url); // 获取连接
    	}
        return connection;
    }

    /**
     * 释放资源
     */
    public  int releaseConn() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -203;
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -204;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -205;
            }
        }
        return 200;
    }

    public  List<Map<String, Object>> getList(String sql) throws SQLException {
    	
        try {
            connection = getConnection();
        } catch (SQLException e) {
            //获取连接失败
            e.printStackTrace();
            return null;
        }
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        //获取查询数据总数
        pstmt = connection.prepareStatement(sql);
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 0; i < cols_len; i++) {
                String cols_name = metaData.getColumnName(i + 1);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_value = "";
                }
                map.put(cols_name, cols_value);
            }
            list.add(map);
        }
        
        int flag = releaseConn();
        if(flag<0){
           System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
        }
        return list;
    }
    
    
    /**
     * @param list
     * @return Map<String,Boolean> key:执行的sql语句    value:true 说明语句有结果集，false 语句没有结果集，如更改
     */
    public  Map<String,Boolean> executeList(List<String> list){
    	Map<String,Boolean> map = new HashMap<String, Boolean>();
    	
    	   try {
               connection = getConnection();
               for (String sql : list) {
            	   pstmt = connection.prepareStatement(sql);
                   boolean execute = pstmt.execute();
                   System.out.println(sql+"===="+execute);
                   map.put(sql, execute);
               }
               
           } catch (SQLException e) {
               e.printStackTrace();
               return null;
           }finally{
        	   int flag = releaseConn();
               if(flag<0){
                  System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
               }
           }
    	return map;
    }

    /**
     * Title: getMap
     * Description: 查询两个字段,返回成map形式
     * @param sql
     * @return
     * @throws SQLException
    */
   public Map<Object,Object> getMap(String sql ) throws SQLException{
   	
		  Map<Object, Object> map = new HashMap<Object, Object>();
		  try {
	          connection = getConnection();
	      } catch (SQLException e) {
	          //获取连接失败
	          e.printStackTrace();
	          return null;
	      }
		  //获取查询数据总数
	      pstmt = connection.prepareStatement(sql);
	      resultSet = pstmt.executeQuery();
	      ResultSetMetaData metaData = resultSet.getMetaData();
	      while (resultSet.next()) {
	    	  String key_name = metaData.getColumnName( 1);
		      Object key = resultSet.getObject(key_name);
		      String value_name = metaData.getColumnName( 2);
		      Object value = resultSet.getObject(value_name);
		      map.put(key, value);
	      }
	      
	      int flag = releaseConn();
	        if(flag<0){
	           System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
	        }
	      return map;
   }

   @Test
   public void test01() throws Exception{
	   
	   ImpalaUtils u = new ImpalaUtils();
	   String sql = "select name from datacube_kudu.d_eshop  ";
	   List<Map<String, Object>> list = u.getList(sql);
	   System.out.println(list.size());
	   for (Map<String, Object> map : list) {
		   System.out.println(map.get("name"));
	}
   }
	
}
