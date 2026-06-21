package com.spark.web.servlet;

import com.spark.web.dao.MysqlDao;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.WebServlet;
import java.io.IOException;
import java.util.Map;

/**
 * 仪表盘主页 Servlet — 加载总览数据
 */
@WebServlet("/dashboard")
public class DashboardServlet extends HttpServlet {

    private final MysqlDao dao = new MysqlDao();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        // 登录检查
        HttpSession session = req.getSession(false);
        if (session == null || session.getAttribute("username") == null) {
            resp.sendRedirect(req.getContextPath() + "/jsp/index.jsp");
            return;
        }

        // 加载总览数据
        Map<String, Object> overview = dao.getDashboardOverview();
        Map<String, Object> trendSummary = dao.getMarketTrendSummary();
        Map<String, Object> devSummary = dao.getDeveloperSummary();

        req.setAttribute("overview", overview);
        req.setAttribute("trendSummary", trendSummary);
        req.setAttribute("devSummary", devSummary);
        req.setAttribute("username", session.getAttribute("username"));

        req.getRequestDispatcher("/jsp/dashboard.jsp").forward(req, resp);
    }
}
