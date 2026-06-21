package com.spark.web.servlet;

import com.google.gson.Gson;
import com.spark.web.dao.MysqlDao;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.WebServlet;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 分析数据 API Servlet — 为前端 AJAX 提供 JSON 数据
 *
 * API 路径:
 *   GET /api/analysis/trend       — 市场趋势数据
 *   GET /api/analysis/pricing     — 定价与评价数据
 *   GET /api/analysis/genres      — 类型标签数据
 *   GET /api/analysis/developers  — 开发商生态数据
 *   GET /api/analysis/overview    — 仪表盘总览数据
 */
@WebServlet("/api/analysis/*")
public class AnalysisServlet extends HttpServlet {

    private final MysqlDao dao = new MysqlDao();
    private final Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json;charset=UTF-8");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        String path = req.getPathInfo();
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            switch (path != null ? path : "/") {
                case "/trend":
                    result.put("status", "ok");
                    result.put("data", dao.getMarketTrend());
                    result.put("summary", dao.getMarketTrendSummary());
                    break;
                case "/pricing":
                    result.put("status", "ok");
                    result.put("data", dao.getPricingReview());
                    break;
                case "/genres":
                    result.put("status", "ok");
                    result.put("genres", dao.getGenreTags());
                    result.put("topGenres", dao.getTopGenres());
                    break;
                case "/developers":
                    result.put("status", "ok");
                    result.put("data", dao.getDeveloperEcosystem());
                    result.put("summary", dao.getDeveloperSummary());
                    break;
                case "/overview":
                    result.put("status", "ok");
                    result.put("overview", dao.getDashboardOverview());
                    result.put("trend", dao.getMarketTrendSummary());
                    result.put("dev", dao.getDeveloperSummary());
                    break;
                default:
                    result.put("status", "error");
                    result.put("message", "Unknown API: " + path);
            }
        } catch (Exception e) {
            result.put("status", "error");
            result.put("message", e.getMessage());
        }

        PrintWriter out = resp.getWriter();
        out.print(gson.toJson(result));
        out.flush();
    }
}
