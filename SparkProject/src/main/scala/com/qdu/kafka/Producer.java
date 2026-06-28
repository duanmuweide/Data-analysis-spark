package com.qdu.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

// MySQL JDBC 相关
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Producer {
    // 定义目标主题名称
    public static String topic = "kafka";

    // ==================== MySQL 连接配置 ====================
    private static final String JDBC_URL = "jdbc:mysql://192.168.211.1:3306/cjz_spark?useSSL=false";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "root";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();

        // 1. 配置 Kafka 集群地址
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master-pc:9091,master-pc:9092,master-pc:9093");

        // 2. Key 和 Value 的序列化方式
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 3. 可靠性配置
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 创建生产者实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        // ==================== 批次轮询：checkid 自增 + 1秒间隔 ====================
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL 驱动加载失败: " + e.getMessage());
            kafkaProducer.close();
            return;
        }

        // ==================== 启动时读取上次进度 ====================
        int currentCheckId = getLastProcessedCheckId();
        System.out.println("========== 启动，从 checkid=" + currentCheckId + " 开始处理 ==========");

        while (true) {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;

            try {
                conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);

                // 检查当前 checkid 是否有数据
                String countSql = "SELECT COUNT(*) AS cnt "
                                + "FROM house_info_clean_checkid "
                                + "WHERE checkid = " + currentCheckId;

                stmt = conn.createStatement();
                rs = stmt.executeQuery(countSql);
                rs.next();
                int cnt = rs.getInt("cnt");
                rs.close();
                stmt.close();

                if (cnt == 0) {
                    System.out.println("无新数据 (checkid=" + currentCheckId + ")");
                } else {
                    // ========== 查询：海淀区 + 90-144㎡ ==========
                    String dataSql = "SELECT community, price_per_sqm "
                                   + "FROM house_info_clean_checkid "
                                   + "WHERE checkid = " + currentCheckId
                                   + "  AND district = '海淀' "
                                   + "  AND area >= 90 AND area < 144 "
                                   + "  AND community IS NOT NULL AND community != '' "
                                   + "  AND price_per_sqm IS NOT NULL AND price_per_sqm > 0";

                    rs = conn.createStatement().executeQuery(dataSql);

                    int sendCount = 0;
                    while (rs.next()) {
                        String community = rs.getString("community");
                        int pricePerSqm = rs.getInt("price_per_sqm");

                        String msg = String.format("{\"community\":\"%s\",\"price_per_sqm\":%d}",
                                                   community, pricePerSqm);
                        kafkaProducer.send(new ProducerRecord<>(topic, msg));
                        sendCount++;
                    }

                    // 发送批次结束标记，Consumer 收到后会触发 MySQL 写入
                    String endMsg = "{\"__END_OF_BATCH__\":true,\"checkid\":" + currentCheckId + "}";
                    kafkaProducer.send(new ProducerRecord<>(topic, endMsg));

                    System.out.println("========== 批次 " + currentCheckId + " 处理完成，发送 " + sendCount + " 条数据 ==========");

                    saveProcessedCheckId(currentCheckId);  // 持久化进度
                    currentCheckId++;  // 批次自增
                }

            } catch (Exception e) {
                System.err.println("异常 (checkid=" + currentCheckId + "): " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (rs != null) try { rs.close(); } catch (Exception e) { }
                if (stmt != null) try { stmt.close(); } catch (Exception e) { }
                if (conn != null) try { conn.close(); } catch (Exception e) { }
            }

            Thread.sleep(1000);  // 每隔 1 秒轮询
        }
    }

    /**
     * 从 MySQL 读取上次处理到的 checkid，重启后断点续跑
     * 返回: 下一个待处理的 checkid（首次运行返回1）
     */
    private static int getLastProcessedCheckId() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
            // 确保进度表存在
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS kafka_producer_progress ("
                       + "id INT PRIMARY KEY DEFAULT 1, "
                       + "last_checkid INT NOT NULL DEFAULT 0)");
            stmt.close();

            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT COALESCE(MAX(last_checkid), 0) AS last_id FROM kafka_producer_progress");
            rs.next();
            int lastId = rs.getInt("last_id");
            return lastId + 1;  // 返回下一个待处理的 checkid
        } catch (Exception e) {
            System.err.println("读取进度失败，默认从1开始: " + e.getMessage());
            return 1;
        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) { }
            if (stmt != null) try { stmt.close(); } catch (Exception e) { }
            if (conn != null) try { conn.close(); } catch (Exception e) { }
        }
    }

    /**
     * 将一个批次处理完毕后，把进度写入 MySQL
     */
    private static void saveProcessedCheckId(int checkId) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
            stmt = conn.createStatement();
            stmt.execute("INSERT INTO kafka_producer_progress (id, last_checkid) VALUES (1, " + checkId + ") "
                       + "ON DUPLICATE KEY UPDATE last_checkid = " + checkId);
        } catch (Exception e) {
            System.err.println("保存进度失败 (checkid=" + checkId + "): " + e.getMessage());
        } finally {
            if (stmt != null) try { stmt.close(); } catch (Exception e) { }
            if (conn != null) try { conn.close(); } catch (Exception e) { }
        }
    }
}
