package com.spark.web.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 房屋分析数据库连接（cjz 项目合并）
 */
public class HouseDbUtil {
    private static final String JDBC_URL = "jdbc:mysql://192.168.211.1:3306/cjz_spark?useSSL=false&characterEncoding=utf8";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "root";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL 驱动加载失败", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    }
}
