<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>出错啦 — Steam 分析平台</title>
    <style>
        body {
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
            background: #f4f6f9;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            text-align: center;
        }
        .error-box {
            background: white;
            padding: 50px;
            border-radius: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        }
        .error-box h1 { font-size: 72px; color: #c62828; margin: 0; }
        .error-box p { font-size: 18px; color: #666; margin: 16px 0; }
        .error-box a { color: #1a237e; text-decoration: none; font-weight: 600; }
    </style>
</head>
<body>
    <div class="error-box">
        <h1>⚠️</h1>
        <p>页面出错了，请稍后重试。</p>
        <a href="<%= request.getContextPath() %>/dashboard">← 返回仪表盘</a>
    </div>
</body>
</html>
