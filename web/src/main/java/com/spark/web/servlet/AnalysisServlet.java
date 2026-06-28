package com.spark.web.servlet;

import com.google.gson.Gson;
import com.spark.web.dao.MysqlDao;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * 分析数据 API Servlet — 供前端 ECharts 加载各分析模块数据
 *
 * GET /api/analysis/market     → A1 市场趋势
 * GET /api/analysis/pricing    → A2 定价与评价
 * GET /api/analysis/genres     → A3 类型标签
 * GET /api/analysis/developer  → A4 开发商生态
 */
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
            if ("/market".equals(path)) {
                result.put("status", "ok");
                result.put("trend", dao.getMarketTrend());
                result.put("summary", dao.getMarketTrendSummary());
            } else if ("/pricing".equals(path)) {
                result.put("status", "ok");
                result.put("data", dao.getPricingReview());
            } else if ("/genres".equals(path)) {
                result.put("status", "ok");
                result.put("tags", dao.getGenreTags());
                result.put("top", dao.getTopGenres());
            } else if ("/developer".equals(path)) {
                result.put("status", "ok");
                result.put("data", dao.getDeveloperEcosystem());
                result.put("summary", dao.getDeveloperSummary());
            } else if ("/overview".equals(path)) {
                result.put("status", "ok");
                result.put("overview", dao.getDashboardOverview());
                result.put("trendSummary", dao.getMarketTrendSummary());
                result.put("devSummary", dao.getDeveloperSummary());
            } else {
                result.put("status", "error");
                result.put("message", "未知接口: " + path + "，可用: /market /pricing /genres /developer /overview");
            }
        } catch (Exception e) {
            result.put("status", "error");
            result.put("message", "查询失败: " + e.getMessage());
        }

        PrintWriter out = resp.getWriter();
        out.print(gson.toJson(result));
        out.flush();
    }
}
