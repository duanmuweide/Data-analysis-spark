package com.spark.web.servlet;

import com.google.gson.Gson;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.WebServlet;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 实时数据 API Servlet — 供前端仪表盘 5 秒轮询
 *
 * 数据来源: Spark Streaming 写入的内存表 / Redis
 * 当前版本返回模拟数据，接入 Kafka + Spark Streaming 后可切换为真实数据
 */
@WebServlet("/api/realtime/*")
public class RealtimeServlet extends HttpServlet {

    private final Gson gson = new Gson();
    private final Random random = new Random();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json;charset=UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("timestamp", System.currentTimeMillis());

        // 模拟实时数据（后续接入 Spark Streaming + Kafka 替换为真实数据）
        result.put("new_games_count", 50 + random.nextInt(30));
        result.put("avg_price", Math.round((15.0 + random.nextDouble() * 20) * 100.0) / 100.0);
        result.put("avg_positive_rate", Math.round((75.0 + random.nextDouble() * 20) * 100.0) / 100.0);

        List<Map<String, Object>> topTags = new ArrayList<>();
        String[] tags = {"Action", "Indie", "Adventure", "RPG", "Strategy",
                         "Simulation", "Casual", "Multiplayer", "Single-player", "FPS"};
        for (String tag : tags) {
            Map<String, Object> tagInfo = new LinkedHashMap<>();
            tagInfo.put("tag", tag);
            tagInfo.put("count", 100 + random.nextInt(500));
            topTags.add(tagInfo);
        }
        topTags.sort((a, b) -> ((Integer)b.get("count")).compareTo((Integer)a.get("count")));
        result.put("top_tags", topTags.subList(0, 8));

        PrintWriter out = resp.getWriter();
        out.print(gson.toJson(result));
        out.flush();
    }
}
