package pers.qingyu.utils;

import java.sql.*;

public class JDBCUtil {
	/*
	 * DBUtil为工具类，其属性为数据库连接中一些配置信息并封装了一些获取与管理数据连接的方案
	 * @author: QingyuMeng
	 */
	public static String ip = "127.168.10.202";
	public static int port = 3306;
	public static String dataBase = "telco";
	public static String encoding = "utf-8";
	public static String loginName = "root";
	public static String passWord = "admin";

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public static Connection getConnection() throws SQLException {
		String url = String.format("jdbc:mysql://%s:%d/%s?characterEncoding=%s", ip, port, dataBase, encoding);
		return (Connection) DriverManager.getConnection(url, loginName, passWord);
	}

	/**
	 * 释放连接器资源
	 */
	public static void close(Connection connection, Statement statement, ResultSet resultSet) {
		try {
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}

			if (statement != null && !statement.isClosed()) {
				statement.close();
			}

			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
