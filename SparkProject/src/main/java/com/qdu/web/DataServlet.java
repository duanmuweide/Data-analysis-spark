package com.qdu.web;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * 统一数据接口 Servlet
 *
 * 请求格式：GET /data?table=area|district|year|community|certain
 * 返回格式：{"success": true/false, "data": [...], "error": "..."}
 *
 * 对应关系：
 *   area      → area_price_analysis        (Spark AreaAnalysis)
 *   district  → district_house_price_analysis (Spark DistrictAnalysis)
 *   year      → house_year_analysis        (Spark YearAnalysis)
 *   community → community_price_analysis   (Spark CommunityAnalysis)
 *   certain   → certain_analysis           (Kafka 实时聚合)
 */
@WebServlet("/data")
public class DataServlet extends HttpServlet {

    private static final Gson GSON = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        // 1. 设置响应头
        resp.setContentType("application/json;charset=UTF-8");
        // 禁用缓存，确保实时轮询拿到最新数据
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        resp.setHeader("Pragma", "no-cache");
        resp.setHeader("Expires", "0");

        // 2. 解析参数
        String table = req.getParameter("table");
        if (table == null || table.isEmpty()) {
            writeError(resp, "缺少参数：?table=area|district|year|community|certain");
            return;
        }

        // 3. 查表并返回 JSON
        String tableName = resolveTableName(table);
        if (tableName == null) {
            writeError(resp, "无效的表名：" + table + "，请使用 area|district|year|community|certain");
            return;
        }

        try (Connection conn = DbUtil.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            JsonArray dataArray = new JsonArray();

            while (rs.next()) {
                JsonObject row = new JsonObject();
                for (int i = 1; i <= colCount; i++) {
                    String colName = meta.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    // 处理 null 值
                    if (value == null) {
                        row.addProperty(colName, (String) null);
                    } else if (value instanceof Number) {
                        row.addProperty(colName, (Number) value);
                    } else if (value instanceof Boolean) {
                        row.addProperty(colName, (Boolean) value);
                    } else {
                        row.addProperty(colName, value.toString());
                    }
                }
                dataArray.add(row);
            }

            JsonObject result = new JsonObject();
            result.addProperty("success", true);
            result.add("data", dataArray);
            result.addProperty("count", dataArray.size());

            PrintWriter out = resp.getWriter();
            out.print(GSON.toJson(result));
            out.flush();

        } catch (Exception e) {
            writeError(resp, "数据库查询异常：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 将前端参数映射到 MySQL 表名
     */
    private String resolveTableName(String table) {
        switch (table) {
            case "area":      return "area_price_analysis";
            case "district":  return "district_house_price_analysis";
            case "year":      return "house_year_analysis";
            case "community": return "community_price_analysis";
            case "certain":   return "certain_analysis";
            default:          return null;
        }
    }

    private void writeError(HttpServletResponse resp, String msg) throws IOException {
        JsonObject result = new JsonObject();
        result.addProperty("success", false);
        result.addProperty("error", msg);
        resp.getWriter().print(GSON.toJson(result));
    }
}
