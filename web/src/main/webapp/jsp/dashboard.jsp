<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%
    String username = (String) session.getAttribute("username");
    if (username == null) {
        response.sendRedirect(request.getContextPath() + "/jsp/index.jsp");
        return;
    }
%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Steam 游戏大数据分析平台</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Microsoft YaHei', sans-serif; background: #f4f6f9; color: #333; }
        header {
            background: linear-gradient(135deg, #1a237e, #0d47a1);
            color: white; padding: 16px 40px;
            display: flex; justify-content: space-between; align-items: center;
        }
        header h1 { font-size: 22px; }
        header a { color: #ffd54f; text-decoration: none; margin-left: 12px; }
        main { max-width: 1300px; margin: 0 auto; padding: 24px 20px; }
        .section { margin-bottom: 28px; }
        .section h2 { font-size: 18px; color: #1a237e; margin-bottom: 12px; padding-bottom: 6px; border-bottom: 2px solid #e8eaf6; }
        .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .chart-box { background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.06); height: 400px; }
        .live-dot { display: inline-block; width: 10px; height: 10px; background: #4caf50; border-radius: 50%; margin-right: 6px; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
        .table-wrap { background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.06); overflow: auto; max-height: 350px; }
        .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
        .data-table th { background: #1a237e; color: white; padding: 8px 10px; text-align: left; position: sticky; top: 0; }
        .data-table td { padding: 6px 10px; border-bottom: 1px solid #e0e0e0; }
        .data-table tr:hover { background: #f5f5f5; }
        footer { text-align: center; padding: 20px; color: #888; font-size: 12px; }
    </style>
</head>
<body>
    <header>
        <h1>Steam 游戏大数据分析平台</h1>
        <div><%= username %> | <a href="<%= request.getContextPath() %>/jsp/index.jsp">退出</a></div>
    </header>
    <main>
        <!-- 4 个离线分析图表 -->
        <div class="chart-row">
            <div class="section"><h2>1. 市场趋势分析</h2><div class="chart-box" id="chart-trend"></div></div>
            <div class="section"><h2>2. 定价与评价关联</h2><div class="chart-box" id="chart-pricing"></div></div>
        </div>
        <div class="chart-row">
            <div class="section"><h2>3. 类型标签挖掘</h2><div class="chart-box" id="chart-genres"></div></div>
            <div class="section"><h2>4. 开发商生态分析</h2><div class="chart-box" id="chart-developers"></div></div>
        </div>

        <!-- Kafka 实时 -->
        <div class="section">
            <h2><span class="live-dot"></span>Kafka 实时数据（累计模式，每10秒刷新）</h2>
            <div class="chart-box" id="chart-realtime" style="height:400px;"></div>
            <div id="realtime-info" style="text-align:center;padding:8px;color:#666;font-size:13px;"></div>
        </div>
    </main>
    <footer>2026年第6学期 Spark 项目 | Steam Games Dataset (115,000+ games)</footer>

    <script>
        var ctx = '<%= request.getContextPath() %>';

        function echartsInit(id) {
            var dom = document.getElementById(id);
            if (!dom) return null;
            var c = echarts.init(dom);
            window.addEventListener('resize', function() { c.resize(); });
            return c;
        }

        async function api(url) {
            try { var r = await fetch(ctx + url); return await r.json(); }
            catch(e) { return null; }
        }

        async function initPage() {
            // A1 - 折线图
            var r1 = await api('/api/analysis/market');
            if (r1 && r1.status === 'ok' && r1.trend) {
                var d = r1.trend;
                var c1 = echartsInit('chart-trend');
                c1.setOption({
                    tooltip: { trigger: 'axis' },
                    legend: { data: ['发布数', '均价($)'], bottom: 0 },
                    xAxis: { type: 'category', data: d.map(function(x) { return x.year; }) },
                    yAxis: { type: 'value' },
                    series: [
                        { name: '发布数', type: 'line', data: d.map(function(x) { return x.release_count; }), smooth: true },
                        { name: '均价($)', type: 'line', data: d.map(function(x) { return x.avg_price; }), smooth: true }
                    ],
                    grid: { left: 50, right: 30, top: 20, bottom: 40 }
                });
            }

            // A2 - 饼图
            var r2 = await api('/api/analysis/pricing');
            if (r2 && r2.status === 'ok' && r2.data) {
                var c2 = echartsInit('chart-pricing');
                c2.setOption({
                    tooltip: { trigger: 'item' },
                    series: [{
                        type: 'pie', radius: ['45%', '75%'],
                        data: r2.data.map(function(x) { return { name: x.price_bucket, value: x.game_count }; }),
                        label: { formatter: '{b}\n{d}%' }
                    }]
                });
            }

            // A3 - 横向柱状图
            var r3 = await api('/api/analysis/genres');
            if (r3 && r3.status === 'ok' && r3.tags) {
                var d3 = r3.tags.slice(0, 20).reverse();
                var c3 = echartsInit('chart-genres');
                c3.setOption({
                    tooltip: { trigger: 'axis' },
                    xAxis: { type: 'value' },
                    yAxis: { type: 'category', data: d3.map(function(x) { return x.tag; }) },
                    series: [{ type: 'bar', data: d3.map(function(x) { return x.frequency; }), itemStyle: { color: '#43a047' } }],
                    grid: { left: 130, right: 20, top: 10, bottom: 20 }
                });
            }

            // A4 - 柱状图
            var r4 = await api('/api/analysis/developer');
            if (r4 && r4.status === 'ok' && r4.data) {
                var d4 = r4.data.slice(0, 15).reverse();
                var c4 = echartsInit('chart-developers');
                c4.setOption({
                    tooltip: { trigger: 'axis' },
                    xAxis: { type: 'value' },
                    yAxis: { type: 'category', data: d4.map(function(x) { return x.developer; }) },
                    series: [{ type: 'bar', data: d4.map(function(x) { return x.game_count; }), itemStyle: { color: '#e65100' } }],
                    grid: { left: 180, right: 20, top: 10, bottom: 20 }
                });
            }

            // 实时
            loadRealtime();
        }

        // 实时累计数据
        var realtimeChart = null;

        async function loadRealtime() {
            if (!realtimeChart) realtimeChart = echartsInit('chart-realtime');
            var d = await api('/api/realtime/latest');

            if (d && d.status === 'ok' && realtimeChart) {
                realtimeChart.setOption({
                    title: { text: 'Kafka 实时累计指标', left: 'center', textStyle: { fontSize: 14 } },
                    tooltip: { trigger: 'axis' },
                    legend: { data: ['新游戏数', '均价($)', '好评率(%)'], bottom: 0 },
                    xAxis: { type: 'category', data: ['累计'] },
                    yAxis: { type: 'value' },
                    series: [
                        { name: '新游戏数', type: 'bar', data: [d.new_games_count || 0], itemStyle: { color: '#1a237e' }, label: { show: true, position: 'top' } },
                        { name: '均价($)', type: 'bar', data: [d.avg_price || 0], itemStyle: { color: '#ff6f00' }, label: { show: true, position: 'top' } },
                        { name: '好评率(%)', type: 'bar', data: [d.avg_positive_rate || 0], itemStyle: { color: '#4caf50' }, label: { show: true, position: 'top' } }
                    ],
                    grid: { left: 60, right: 20, top: 30, bottom: 40 }
                });
                document.getElementById('realtime-info').innerHTML = '累计游戏数: <b>' + (d.new_games_count || 0) + '</b> | 均价: <b>$' + (d.avg_price || 0) + '</b> | 好评率: <b>' + (d.avg_positive_rate || 0) + '%</b>';
            }
        }

        document.addEventListener('DOMContentLoaded', function() {
            initPage();
            setInterval(loadRealtime, 5000);
        });
    </script>
</body>
</html>
