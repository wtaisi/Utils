package com.jdbc.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;


public final class JDBCMysqlConnectionUtils {
	private static final Logger log=Logger.getLogger(JDBCMysqlConnectionUtils.class);
	
	private static final String driver =getPropertyValueByKey("jdbc.mysql.driver");
	private static final String url =getPropertyValueByKey("jdbc.mysql.url");
	private static final String username =getPropertyValueByKey("jdbc.mysql.username");
	private static final String password =getPropertyValueByKey("jdbc.mysql.password");
	
	public static void main(String[] args) throws SQLException{
		System.out.println(getConnection());
	}
	
	public static Connection getConnection(String driver, String username, String password, String url){
		Connection con = null;
			try {
				Class.forName(driver);
				con = DriverManager.getConnection(url, username, password);
				log.info(con);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		return con;
	}
	/**
	 * 获取数据库连接
	 * */
	public static Connection getConnection(){
		return getConnection(driver,username,password,url);
	}
	
	public static String getPropertyValueByKey(String key){
		Properties props = new Properties();
		try {
			props.load(new InputStreamReader(new FileInputStream("resources\\application.properties"), "UTF-8"));
		} catch (FileNotFoundException e) {
			try {
				props.load(new InputStreamReader(JDBCMysqlConnectionUtils.class.getClassLoader().getResourceAsStream("application.properties"),"UTF-8"));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return props.getProperty(key);
	}
	
}
