<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Steam 游戏大数据分析平台 — 登录</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #1a237e 0%, #0d47a1 50%, #01579b 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .login-container {
            background: white;
            border-radius: 16px;
            padding: 50px 40px;
            width: 420px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            text-align: center;
        }
        .login-container h1 {
            font-size: 28px;
            color: #1a237e;
            margin-bottom: 8px;
        }
        .login-container .subtitle {
            color: #666;
            font-size: 14px;
            margin-bottom: 32px;
        }
        .login-container .emoji {
            font-size: 48px;
            margin-bottom: 16px;
        }
        .form-group {
            margin-bottom: 20px;
            text-align: left;
        }
        .form-group label {
            display: block;
            font-size: 14px;
            color: #333;
            margin-bottom: 6px;
            font-weight: 600;
        }
        .form-group input {
            width: 100%;
            padding: 12px 16px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 15px;
            transition: border-color 0.3s;
            outline: none;
        }
        .form-group input:focus {
            border-color: #1a237e;
        }
        .btn-login {
            width: 100%;
            padding: 14px;
            background: linear-gradient(135deg, #1a237e, #0d47a1);
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
            margin-top: 8px;
        }
        .btn-login:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(26,35,126,0.4);
        }
        .error-msg {
            background: #ffebee;
            color: #c62828;
            padding: 10px 16px;
            border-radius: 6px;
            margin-bottom: 16px;
            font-size: 14px;
        }
        .footer {
            margin-top: 24px;
            color: #999;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="emoji">🎮</div>
        <h1>Steam 游戏大数据分析平台</h1>
        <p class="subtitle">2026年第6学期 · Spark 项目</p>

        <% if (request.getAttribute("error") != null) { %>
            <div class="error-msg"><%= request.getAttribute("error") %></div>
        <% } %>

        <form action="<%= request.getContextPath() %>/login" method="post">
            <div class="form-group">
                <label for="username">👤 用户名</label>
                <input type="text" id="username" name="username"
                       placeholder="请输入用户名" required autofocus>
            </div>
            <div class="form-group">
                <label for="password">🔒 密码</label>
                <input type="password" id="password" name="password"
                       placeholder="请输入密码" required>
            </div>
            <button type="submit" class="btn-login">登 录</button>
        </form>

        <p class="footer">数据集: Steam Games (115,000+ 游戏, 390MB)</p>
    </div>
</body>
</html>
