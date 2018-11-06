package com.gasq.bdp.task.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class HiveService {
       static Logger logger = Logger.getLogger(HiveService.class);
       //hive的jdbc驱动类
       public static String dirverName = "org.apache.hive.jdbc.HiveDriver"; 
//       //连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive 
       public static String url = "jdbc:hive2://slave1:21050/;auth=noSasl";
//       //登录linux的用户名  一般会给权限大一点的用户，否则无法进行事务形操作
       public static String user = "hive";
//       //登录linux的密码
       public static String pass = "hive";
       
       public static String dbname = "default";
       
       public static Connection conn = null;
       public static Statement stmt = null;
       
	   public static ResultSet getResultSet(String db,String queryHql) throws Exception {
	   		try {
	   			conn = HiveService.getDefConn();
	   			stmt = HiveService.getStmt(conn);
	   			stmt.execute("use "+((db==null)?"default":db));
	   			return stmt.executeQuery(queryHql);
	   		} catch (SQLException e) {
	   			throw new Exception("连接impala数据库出错！",e);
	   		}
	   	}
       public static Connection getDefConn(){
           Connection conn = null;
           try {
                  Class.forName(dirverName);
                  conn = DriverManager.getConnection(url, user, pass);
           } catch (ClassNotFoundException e) {
                  e.printStackTrace();
           } catch (SQLException e) {
                  e.printStackTrace();
           }
           return conn;
    }
       
       /**
        * 创建连接
        * @return
        * @throws SQLException
        */
       public static Connection getConn(String dirverName,String url,String user,String pass){
              Connection conn = null;
              try {
                     Class.forName(dirverName);
                     conn = DriverManager.getConnection(url, user, pass);
              } catch (ClassNotFoundException e) {
                     e.printStackTrace();
              } catch (SQLException e) {
                     e.printStackTrace();
              }
              return conn;
       }
       
       /**
        * 创建命令
        * @param conn
        * @return
        * @throws SQLException
        */
       public static Statement getStmt(Connection conn) throws SQLException{
              logger.debug(conn);
              if(conn == null){
                     logger.debug("this conn is null");
              }
              return conn.createStatement();
       }
       
       /**
        * 关闭连接
        * @param conn
        */
       public static void closeConn(Connection conn){
              try {
                     conn.close();
              } catch (SQLException e) {
                     e.printStackTrace();
              }
       }
       
       /**
        * 关闭命令
        * @param stmt
        */
       public static void closeStmt(Statement stmt){
              try {
                     stmt.close();
              } catch (SQLException e) {
                     e.printStackTrace();
              }
       }
       
       public static boolean exeHiveOption(String db,String hql) throws Exception {
	   		conn = HiveService.getDefConn();
	   		try {
	   			stmt = HiveService.getStmt(conn);
	   		} catch (SQLException e) {
	   			if(conn !=null) {closeConn(conn);}
	   			throw new Exception("链接失败！"+e.getMessage(),e);
	   		}
	   		try {
	   			stmt.execute("use "+db);
	   			logger.info("create is susscess");
	   			return stmt.execute(hql);
	   		} catch (SQLException e) {
	   			if(!hql.toLowerCase().trim().startsWith("drop")) throw new Exception("执行Hive语句失败！"+e.getMessage(),e);
	   		}finally {
	   			if(stmt !=null) {closeStmt(stmt);}
	   			if(conn !=null) {closeConn(conn);}
			}
	   		return false;
	   	}
       
        public static void loadFileInHive(String db,String filepath,String tablename) throws Exception {
        	String hql = "load data inpath '" + filepath + "' OVERWRITE into table " + tablename;
        	exeHiveOption(db, hql);
        }
		public static void closeConn() {
			if(stmt !=null) {HiveService.closeStmt(stmt);}
			if(conn !=null) {HiveService.closeConn(conn);}
		}
       	
//       public static void main(String[] args) throws Exception {
//           Class.forName(dirverName);
//           Connection connection = DriverManager.getConnection(url, user,pass);
//           Statement stmt = connection.createStatement();
//           String querySQL="select * from f_customer_pro_suorder_new_action";
//           ResultSet resut = stmt.executeQuery(querySQL);
//           while (resut.next()) {
//               System.out.println(resut.getInt(1));
//           }
//       }
}