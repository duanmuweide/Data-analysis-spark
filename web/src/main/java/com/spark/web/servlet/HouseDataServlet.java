package com.spark.web.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.spark.web.dao.HouseDbUtil;

import javax.servlet.ServletException;
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
 * 房屋分析数据接口（cjz 项目合并到 wade 主项目）
 *
 * GET /house/data?table=area|district|year|community|certain
 */
public class HouseDataServlet extends HttpServlet {

    private static final Gson GSON = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json;charset=UTF-8");
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        String table = req.getParameter("table");
        if (table == null || table.isEmpty()) {
            writeError(resp, "缺少参数：?table=area|district|year|community|certain");
            return;
        }

        String tableName = resolveTableName(table);
        if (tableName == null) {
            writeError(resp, "无效表名：" + table);
            return;
        }

        try (Connection conn = HouseDbUtil.getConnection();
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
                    if (value == null) row.addProperty(colName, (String) null);
                    else if (value instanceof Number) row.addProperty(colName, (Number) value);
                    else row.addProperty(colName, value.toString());
                }
                dataArray.add(row);
            }

            JsonObject result = new JsonObject();
            result.addProperty("success", true);
            result.add("data", dataArray);
            PrintWriter out = resp.getWriter();
            out.print(GSON.toJson(result));
            out.flush();
        } catch (Exception e) {
            writeError(resp, "查询异常：" + e.getMessage());
        }
    }

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
