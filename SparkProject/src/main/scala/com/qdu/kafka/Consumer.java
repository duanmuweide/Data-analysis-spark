package com.qdu.kafka;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;

// MySQL JDBC 相关
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

// 聚合相关
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {
    // 定义要订阅的主题名称
    public static String topic = "kafka";

    // ==================== MySQL 连接配置 ====================
    private static final String JDBC_URL = "jdbc:mysql://192.168.211.1:3306/cjz_spark?useSSL=false";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "root";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    // ==================== 内存聚合容器 ====================
    // Key: 小区名, Value: [房屋数量, 单价总和]
    private static final ConcurrentHashMap<String, long[]> communityStats = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL 驱动加载失败: " + e.getMessage());
            return;
        }

        Properties p = new Properties();

        // 1. 配置 Kafka 集群地址
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master-pc:9091,master-pc:9092,master-pc:9093");

        // 2. Key 和 Value 的反序列化方式
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 3. 消费者组 ID
        p.put(ConsumerConfig.GROUP_ID_CONFIG, topic);

        // 4. 自动重置偏移量策略
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建消费者实例
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(p);
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        System.out.println("========== Consumer 启动，监听 Kafka 主题: " + topic + " ==========");

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String msg = record.value();
                    System.out.println(String.format("topic:%s, offset:%d, 消息:%s",
                            record.topic(), record.offset(), msg));

                    if (msg.contains("__END_OF_BATCH__")) {
                        // ========== 收到批次结束标记 → 写入 MySQL ==========
                        int batchCheckId = Integer.parseInt(extractJsonValue(msg, "checkid"));
                        System.out.println("========== 收到批次 " + batchCheckId + " 结束标记，开始写入 MySQL ==========");
                        flushToMySQL();
                        communityStats.clear();  // 清空为下一批次准备
                    } else {
                        // ========== 普通数据消息 → 内存聚合 ==========
                        aggregateMessage(msg);
                    }
                }
            }
        } finally {
            if (!communityStats.isEmpty()) {
                flushToMySQL();
            }
            kafkaConsumer.close();
        }
    }

    /**
     * 解析消息并累加到内存聚合容器
     */
    private static void aggregateMessage(String msg) {
        try {
            String community = extractJsonValue(msg, "community");
            int pricePerSqm = Integer.parseInt(extractJsonValue(msg, "price_per_sqm"));

            if (community == null || community.isEmpty()) {
                return;
            }

            communityStats.compute(community, (key, old) -> {
                if (old == null) {
                    return new long[]{1L, pricePerSqm};
                } else {
                    old[0] += 1;
                    old[1] += pricePerSqm;
                    return old;
                }
            });
        } catch (Exception e) {
            System.err.println("消息解析失败: " + msg + ", " + e.getMessage());
        }
    }

    /**
     * 简易 JSON 值提取（不依赖第三方 JSON 库）
     */
    private static String extractJsonValue(String json, String key) {
        String searchKey = "\"" + key + "\":";
        int idx = json.indexOf(searchKey);
        if (idx == -1) throw new RuntimeException("未找到 key: " + key);

        int start = idx + searchKey.length();
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) start++;

        if (json.charAt(start) == '"') {
            int end = json.indexOf('"', start + 1);
            return json.substring(start + 1, end);
        } else {
            int end = start;
            while (end < json.length() &&
                   (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) end++;
            return json.substring(start, end);
        }
    }

    /**
     * 将聚合结果写入 MySQL
     * INSERT ... ON DUPLICATE KEY UPDATE 实现幂等，重复执行不脏数据
     */
    private static void flushToMySQL() {
        if (communityStats.isEmpty()) {
            System.out.println("========== 无聚合数据，跳过写入 ==========");
            return;
        }

        String sql = "INSERT INTO certain_analysis (district, area, community, number, averageprice) "
                   + "VALUES ('海淀', '90-144㎡', ?, ?, ?) "
                   + "ON DUPLICATE KEY UPDATE "
                   + "  number = number + VALUES(number), "
                   + "  averageprice = CAST((averageprice * number + VALUES(averageprice) * VALUES(number)) "
                   + "                         / (number + VALUES(number)) AS SIGNED)";

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (Map.Entry<String, long[]> entry : communityStats.entrySet()) {
                String community = entry.getKey();
                long[] stats = entry.getValue();
                int count = (int) stats[0];
                int avgPrice = (int) (stats[1] / stats[0]);

                pstmt.setString(1, community);
                pstmt.setInt(2, count);
                pstmt.setInt(3, avgPrice);
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            System.out.println("========== MySQL 写入完成，共 " + communityStats.size() + " 个小区 ==========");

        } catch (SQLException e) {
            System.err.println("MySQL 写入失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
