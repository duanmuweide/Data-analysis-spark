package com.qdu.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 数据库连接工具类
 * 提供统一的 JDBC 连接获取方式，所有 Web 层通过此类访问 MySQL
 */
public class DbUtil {

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

    /**
     * 获取一个新的数据库连接
     * 调用方负责在 finally 中 close()
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    }
}
