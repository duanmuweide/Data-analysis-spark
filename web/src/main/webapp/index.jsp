<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>2026年 Spark 大数据分析项目</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #0d1117 100%);
            color: #e6edf3; min-height: 100vh;
        }
        .hero {
            text-align: center; padding: 60px 20px 40px;
            background: linear-gradient(180deg, rgba(88,166,255,0.08) 0%, transparent 100%);
        }
        .hero h1 { font-size: 36px; margin-bottom: 12px; }
        .hero p { color: #8b949e; font-size: 16px; }
        .container { max-width: 1000px; margin: 0 auto; padding: 20px; }
        .cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; margin-top: 30px; }
        .card {
            background: #161b22; border: 1px solid #30363d; border-radius: 16px;
            padding: 32px 24px; text-align: center; transition: all 0.3s;
            text-decoration: none; color: inherit; display: block;
        }
        .card:hover { border-color: #58a6ff; transform: translateY(-4px); box-shadow: 0 12px 40px rgba(88,166,255,0.15); }
        .card .icon { font-size: 48px; margin-bottom: 16px; }
        .card h3 { font-size: 20px; margin-bottom: 8px; color: #58a6ff; }
        .card p { color: #8b949e; font-size: 14px; line-height: 1.8; }
        .card .tag { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; margin-top: 12px; }
        .tag-spark { background: rgba(88,166,255,0.15); color: #58a6ff; }
        .tag-kafka { background: rgba(63,185,80,0.15); color: #3fb950; }
        .tag-web { background: rgba(210,153,34,0.15); color: #d29922; }
        .members { text-align: center; margin-top: 50px; padding: 20px; color: #8b949e; font-size: 14px; }
        .members span { color: #58a6ff; margin: 0 8px; }
        footer { text-align: center; padding: 40px 20px; color: #484f58; font-size: 12px; }
    </style>
</head>
<body>
    <div class="hero">
        <h1>2026 年第 6 学期 Spark 大数据分析项目</h1>
        <p>Spark 离线分析 + Kafka 实时流 + Web 仪表盘</p>
    </div>

    <div class="container">
        <div class="cards">
            <!-- 项目一：Steam -->
            <a href="jsp/index.jsp" class="card">
                <div class="icon">🎮</div>
                <h3>Steam 游戏分析</h3>
                <p>市场趋势 · 定价评价 · 类型挖掘 · 开发商生态</p>
                <span class="tag tag-spark">Spark SQL</span>
                <span class="tag tag-kafka">Kafka 实时</span>
                <span class="tag tag-web">ECharts</span>
            </a>

            <!-- 项目二：房屋价格分析 (cjz) -->
            <a href="house/index.jsp" class="card">
                <div class="icon">🏠</div>
                <h3>房屋价格分析</h3>
                <p>面积区间 · 市区对比 · 年份户型 · 小区排名</p>
                <span class="tag tag-spark">Spark SQL</span>
                <span class="tag tag-kafka">Kafka 实时</span>
                <span class="tag tag-web">ECharts</span>
            </a>

            <!-- 项目三：电商数据分析 (czm) -->
            <a href="http://127.0.0.1:5000" class="card">
                <div class="icon">🛒</div>
                <h3>电商数据分析</h3>
                <p>用户行为 · 销售趋势 · 品类挖掘 · 画像分析</p>
                <span class="tag tag-spark">Spark SQL</span>
                <span class="tag tag-kafka">Kafka 实时</span>
                <span class="tag tag-web">Web 仪表盘</span>
            </a>
        </div>
    </div>

    <div class="members">
        小组：<span>wade</span> · <span>czm</span> · <span>cjz</span>
    </div>

    <footer>2026 第 6 学期 Spark 项目 | Steam · 房屋 · 电商</footer>
</body>
</html>
