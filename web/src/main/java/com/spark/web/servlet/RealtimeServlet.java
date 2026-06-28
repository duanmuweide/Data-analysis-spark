package com.spark.web.servlet;

import com.google.gson.Gson;
import com.spark.web.dao.MysqlDao;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 实时数据 API Servlet — 供前端仪表盘 5 秒轮询
 *
 * 数据来源: Spark Structured Streaming → MySQL
 *   - realtime_game_stats  : 最新窗口聚合指标
 *   - realtime_genre_counts: 最新窗口类型分布
 *
 * 返回格式:
 *   {
 *     "status": "ok",
 *     "timestamp": 1719000000000,
 *     "new_games_count": 45,
 *     "avg_price": 12.99,
 *     "avg_owners": 150000,
 *     "avg_positive_rate": 82.5,
 *     "window_start": "...",
 *     "window_end": "...",
 *     "top_tags": [{ "tag": "Action", "count": 120 }, ...]
 *   }
 */
public class RealtimeServlet extends HttpServlet {

    private final MysqlDao dao = new MysqlDao();
    private final Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json;charset=UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", System.currentTimeMillis());

        try {
            // 从 MySQL 读取最新实时统计数据
            Map<String, Object> stats = dao.getRealtimeStats();

            if (stats != null && !stats.isEmpty()) {
                result.put("status", "ok");
                result.put("new_games_count", stats.getOrDefault("new_games_count", 0));
                result.put("avg_price", stats.getOrDefault("avg_price", 0.0));
                result.put("avg_owners", stats.getOrDefault("avg_owners", 0L));
                result.put("avg_positive_rate", stats.getOrDefault("avg_positive_rate", 0.0));
                result.put("window_start", stringOrNull(stats.get("window_start")));
                result.put("window_end", stringOrNull(stats.get("window_end")));

                // 读取实时类型分布
                List<Map<String, Object>> genreRows = dao.getRealtimeTopGenres();
                List<Map<String, Object>> topTags = new ArrayList<>();
                if (genreRows != null) {
                    for (Map<String, Object> row : genreRows) {
                        Map<String, Object> tagInfo = new LinkedHashMap<>();
                        tagInfo.put("tag", row.getOrDefault("genre", "Unknown"));
                        tagInfo.put("count", row.getOrDefault("count", 0));
                        topTags.add(tagInfo);
                    }
                }
                result.put("top_tags", topTags);
            } else {
                // 尚无实时数据（Spark Streaming 未启动或尚无 Kafka 消息到达）
                result.put("status", "waiting");
                result.put("message", "暂无实时数据，请确认 Spark Streaming 已启动且 Kafka 正在推送数据");
                result.put("new_games_count", 0);
                result.put("avg_price", 0.0);
                result.put("avg_positive_rate", 0.0);
                result.put("top_tags", new ArrayList<>());
            }

        } catch (Exception e) {
            result.put("status", "error");
            result.put("message", "查询实时数据失败: " + e.getMessage());
            result.put("new_games_count", 0);
            result.put("avg_price", 0.0);
            result.put("avg_positive_rate", 0.0);
            result.put("top_tags", new ArrayList<>());
        }

        PrintWriter out = resp.getWriter();
        out.print(gson.toJson(result));
        out.flush();
    }

    /** 安全转字符串，null → null */
    private String stringOrNull(Object obj) {
        return obj != null ? obj.toString() : null;
    }
}
