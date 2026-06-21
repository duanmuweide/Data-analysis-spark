package com.spark.web.servlet;

import com.spark.web.dao.MysqlDao;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.WebServlet;
import java.io.IOException;

/**
 * 登录 Servlet — 简单的用户验证
 *
 * 参考上学期 Data-analysis 项目的 LoginServlet 设计
 */
@WebServlet("/login")
public class LoginServlet extends HttpServlet {

    private final MysqlDao dao = new MysqlDao();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        // 已登录则直接进入仪表盘
        HttpSession session = req.getSession(false);
        if (session != null && session.getAttribute("username") != null) {
            resp.sendRedirect(req.getContextPath() + "/dashboard");
            return;
        }
        req.getRequestDispatcher("/jsp/index.jsp").forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");

        String username = req.getParameter("username");
        String password = req.getParameter("password");

        if (dao.validateUser(username, password)) {
            HttpSession session = req.getSession();
            session.setAttribute("username", username);
            session.setAttribute("loginTime", System.currentTimeMillis());
            resp.sendRedirect(req.getContextPath() + "/dashboard");
        } else {
            req.setAttribute("error", "用户名或密码错误");
            req.getRequestDispatcher("/jsp/index.jsp").forward(req, resp);
        }
    }
}
