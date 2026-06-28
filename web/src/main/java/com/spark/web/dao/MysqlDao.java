package com.spark.web.dao;

import java.sql.*;
import java.util.*;

/**
 * MySQL 数据访问层 — 读取 Spark 分析结果
 *
 * 数据库: spark_steam_games
 * 各分析结果表在 Spark 分析完成后写入
 *
 * 表结构:
 *   - analysis_market_trend        : A1 市场趋势（离线批处理）
 *   - analysis_pricing_review      : A2 定价与评价（离线批处理）
 *   - analysis_genre_tags          : A3 类型标签挖掘（离线批处理）
 *   - analysis_developer_ecosystem : A4 开发商生态（离线批处理）
 *   - realtime_game_stats          : 实时窗口聚合指标（流处理 → 10秒更新）
 *   - realtime_genre_counts        : 实时类型分布（流处理 → 10秒更新）
 */
public class MysqlDao {

    private static final String JDBC_URL = "jdbc:mysql://192.168.211.1:3306/spark_steam_games?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC Driver not found", e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
    }

    // ============================================================
    // 用户验证
    // ============================================================

    public boolean validateUser(String username, String password) {
        // 简单验证（生产环境应使用加密 + 数据库用户表）
        return username != null && !username.trim().isEmpty()
            && password != null && !password.trim().isEmpty();
    }

    // ============================================================
    // 仪表盘总览数据
    // ============================================================

    public Map<String, Object> getDashboardOverview() {
        Map<String, Object> overview = new LinkedHashMap<>();
        overview.put("totalGames", querySingleLong(
            "SELECT COUNT(*) FROM analysis_market_trend"));
        overview.put("totalYears", querySingleLong(
            "SELECT COUNT(*) FROM analysis_market_trend"));
        overview.put("freeGamesPct", querySingleDouble(
            "SELECT AVG(CASE WHEN Price = 0 THEN 1.0 ELSE 0.0 END) * 100 FROM analysis_market_trend"));
        overview.put("topDeveloper", querySingleString(
            "SELECT Developers FROM analysis_developer_ecosystem ORDER BY game_count DESC LIMIT 1"));
        return overview;
    }

    // ============================================================
    // Analysis 1: 市场趋势
    // ============================================================

    public List<Map<String, Object>> getMarketTrend() {
        return queryList(
            "SELECT year, release_count, avg_owners, avg_price, avg_playtime, " +
            "yoy_growth_pct, yoy_growth_category " +
            "FROM analysis_market_trend ORDER BY year ASC"
        );
    }

    public Map<String, Object> getMarketTrendSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("peakYear", querySingleLong(
            "SELECT year FROM analysis_market_trend ORDER BY release_count DESC LIMIT 1"));
        summary.put("peakCount", querySingleLong(
            "SELECT MAX(release_count) FROM analysis_market_trend"));
        summary.put("avgAnnualGames", querySingleLong(
            "SELECT CAST(AVG(release_count) AS SIGNED) FROM analysis_market_trend"));
        summary.put("latestYearGrowth", querySingleDouble(
            "SELECT yoy_growth_pct FROM analysis_market_trend ORDER BY year DESC LIMIT 1"));
        return summary;
    }

    // ============================================================
    // Analysis 2: 定价与评价
    // ============================================================

    public List<Map<String, Object>> getPricingReview() {
        return queryList(
            "SELECT price_bucket, game_count, game_pct, avg_positive_rate_pct, " +
            "avg_metacritic_score, avg_recommendations, avg_owners, owners_share_pct " +
            "FROM analysis_pricing_review ORDER BY game_count DESC"
        );
    }

    // ============================================================
    // Analysis 3: 类型标签
    // ============================================================

    public List<Map<String, Object>> getGenreTags() {
        return queryList(
            "SELECT tag, frequency, avg_owners, total_games " +
            "FROM analysis_genre_tags ORDER BY frequency DESC LIMIT 50"
        );
    }

    public List<Map<String, Object>> getTopGenres() {
        return queryList(
            "SELECT tag AS genre, frequency, avg_owners " +
            "FROM analysis_genre_tags ORDER BY avg_owners DESC LIMIT 20"
        );
    }

    // ============================================================
    // Analysis 4: 开发商生态
    // ============================================================

    public List<Map<String, Object>> getDeveloperEcosystem() {
        return queryList(
            "SELECT * FROM analysis_developer_ecosystem ORDER BY game_count DESC LIMIT 30"
        );
    }

    public Map<String, Object> getDeveloperSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("marketStructure", querySingleString(
            "SELECT market_structure FROM analysis_developer_ecosystem ORDER BY game_count DESC LIMIT 1"));
        summary.put("topDev", querySingleString(
            "SELECT developer FROM analysis_developer_ecosystem ORDER BY game_count DESC LIMIT 1"));
        summary.put("topDevCount", querySingleLong(
            "SELECT MAX(game_count) FROM analysis_developer_ecosystem"));
        summary.put("totalDevs", querySingleLong(
            "SELECT COUNT(*) FROM analysis_developer_ecosystem"));
        summary.put("hhi", querySingleDouble(
            "SELECT MAX(market_hhi) FROM analysis_developer_ecosystem"));
        return summary;
    }

    // ============================================================
    // 实时数据（Spark Streaming → MySQL）
    // ============================================================

    /**
     * 获取最新窗口实时统计指标
     *
     * @return Map 包含 window_start, window_end, new_games_count,
     *         avg_price, avg_owners, avg_positive_rate, updated_at
     */
    public Map<String, Object> getRealtimeStats() {
        List<Map<String, Object>> rows = queryList(
            "SELECT window_start, window_end, new_games_count, " +
            "avg_price, avg_owners, avg_positive_rate, updated_at " +
            "FROM realtime_game_stats " +
            "ORDER BY window_end DESC LIMIT 1"
        );
        if (rows != null && !rows.isEmpty()) {
            return rows.get(0);
        }
        return null; // 返回 null 表示尚无实时数据
    }

    /**
     * 获取最新窗口的类型分布（Top 10）
     *
     * @return List<Map> 每项包含 genre, count, window_start, window_end
     */
    public List<Map<String, Object>> getRealtimeTopGenres() {
        return queryList(
            "SELECT genre, count, window_start, window_end " +
            "FROM realtime_genre_counts " +
            "ORDER BY count DESC LIMIT 10"
        );
    }

    /**
     * 检查是否有实时数据
     */
    public boolean hasRealtimeData() {
        long count = querySingleLong(
            "SELECT COUNT(*) FROM realtime_game_stats"
        );
        return count > 0;
    }

    // ============================================================
    // 查询工具方法
    // ============================================================

    private List<Map<String, Object>> queryList(String sql) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                result.add(row);
            }
        } catch (SQLException e) {
            System.err.println("[MysqlDao] Query error: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    private long querySingleLong(String sql) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            System.err.println("[MysqlDao] Query error: " + e.getMessage());
        }
        return 0L;
    }

    private double querySingleDouble(String sql) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return rs.getDouble(1);
        } catch (SQLException e) {
            System.err.println("[MysqlDao] Query error: " + e.getMessage());
        }
        return 0.0;
    }

    private String querySingleString(String sql) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return rs.getString(1);
        } catch (SQLException e) {
            System.err.println("[MysqlDao] Query error: " + e.getMessage());
        }
        return "N/A";
    }
}
