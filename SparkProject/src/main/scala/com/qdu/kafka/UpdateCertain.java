package com.qdu.kafka;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 向 house_info_clean_checkid 插入一条新数据，触发完整实时链路：
 *
 *   house_info_clean_checkid → Producer → Kafka → Consumer → certain_analysis → 前端（3秒刷新）
 */
public class UpdateCertain {

    private static final String JDBC_URL  = "jdbc:mysql://192.168.211.1:3306/cjz_spark?useSSL=false&characterEncoding=utf8";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASS = "root";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public static void main(String[] args) {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL 驱动加载失败: " + e.getMessage());
            return;
        }

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {

            // 1. Producer 下一个要处理的 checkid = 上次进度 + 1
            int newCheckId = 1;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COALESCE(MAX(last_checkid), 0) + 1 FROM kafka_producer_progress")) {
                if (rs.next()) {
                    newCheckId = rs.getInt(1);
                }
            }
            System.out.println("Producer 下一个等待的 checkid = " + newCheckId);

            // 2. 插入新行
            // Producer 过滤条件: district='海淀' AND area>=90 AND area<144 AND community 不为空 AND price_per_sqm>0
            String insertSql = "INSERT INTO house_info_clean_checkid (checkid, district, area, community, price_per_sqm) "
                             + "VALUES (?, '海淀', 100, '世茂城', 3788100)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, newCheckId);
                pstmt.executeUpdate();
            }

            System.out.println("插入成功: checkid=" + newCheckId + ", district=海淀, area=100, community=世茂城, price_per_sqm=3788100");
            System.out.println("");
            System.out.println("数据流: house_info_clean_checkid → Producer(1秒内) → Kafka → Consumer → certain_analysis → 前端(3秒刷新)");

            // 3. 检查 Producer 当前进度
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COALESCE(MAX(last_checkid), 0) FROM kafka_producer_progress")) {
                if (rs.next()) {
                    int progress = rs.getInt(1);
                    System.out.println("");
                    System.out.println("Producer 当前已处理到 checkid = " + progress + "，新数据 checkid = " + newCheckId);
                    if (newCheckId <= progress) {
                        System.out.println("WARNING: 新 checkid 已被处理过，Producer 不会重复消费");
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("操作失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
