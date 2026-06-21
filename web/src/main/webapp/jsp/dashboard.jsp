<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.util.*" %>
<%
    String username = (String) session.getAttribute("username");
    if (username == null) {
        response.sendRedirect(request.getContextPath() + "/jsp/index.jsp");
        return;
    }
    Map<String, Object> overview = (Map<String, Object>) request.getAttribute("overview");
    Map<String, Object> trendSummary = (Map<String, Object>) request.getAttribute("trendSummary");
    Map<String, Object> devSummary = (Map<String, Object>) request.getAttribute("devSummary");
%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Steam 游戏大数据分析平台 — 仪表盘</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; background: #f4f6f9; color: #333; }
        header {
            background: linear-gradient(135deg, #1a237e, #0d47a1);
            color: white; padding: 16px 40px;
            display: flex; justify-content: space-between; align-items: center;
        }
        header h1 { font-size: 22px; }
        header .user-info { font-size: 14px; opacity: 0.9; }
        header .user-info a { color: #ffd54f; text-decoration: none; margin-left: 12px; }
        main { max-width: 1300px; margin: 0 auto; padding: 24px 20px; }
        .summary-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 18px; margin-bottom: 28px; }
        .summary-card {
            background: white; border-radius: 12px; padding: 22px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
            text-align: center; transition: transform 0.2s;
        }
        .summary-card:hover { transform: translateY(-3px); }
        .summary-card .value { font-size: 32px; font-weight: 700; color: #1a237e; }
        .summary-card .label { font-size: 13px; color: #888; margin-top: 6px; }
        .section { margin-bottom: 32px; }
        .section h2 { font-size: 20px; color: #1a237e; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #e8eaf6; }
        .analysis-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 22px; margin-bottom: 28px; }
        .analysis-card {
            background: white; border-radius: 12px; padding: 24px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
            cursor: pointer; transition: transform 0.2s, box-shadow 0.2s;
            text-decoration: none; color: inherit; display: block;
        }
        .analysis-card:hover { transform: translateY(-4px); box-shadow: 0 8px 24px rgba(0,0,0,0.12); }
        .analysis-card h3 { color: #1a237e; margin-bottom: 8px; font-size: 18px; }
        .analysis-card .icon { font-size: 36px; margin-bottom: 10px; }
        .analysis-card p { color: #666; font-size: 14px; line-height: 1.6; }
        .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 22px; }
        .chart-box { background: white; border-radius: 12px; padding: 16px; height: 380px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .data-table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 14px; }
        .data-table th { background: #1a237e; color: white; padding: 10px 12px; text-align: left; }
        .data-table td { padding: 8px 12px; border-bottom: 1px solid #e0e0e0; }
        .data-table tr:hover { background: #f5f5f5; }
        .tag { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
        .tag-growth { background: #e8f5e9; color: #2e7d32; }
        .tag-decline { background: #ffebee; color: #c62828; }
        .tag-stable { background: #e3f2fd; color: #1565c0; }
        footer { text-align: center; padding: 20px; color: #888; font-size: 12px; }
    </style>
</head>
<body>
    <header>
        <h1>🎮 Steam 游戏大数据分析平台</h1>
        <div class="user-info">
            👤 <%= username %> | <a href="<%= request.getContextPath() %>/jsp/index.jsp">退出</a>
        </div>
    </header>

    <main>
        <!-- ====== 数据概览卡片 ====== -->
        <div class="summary-cards">
            <div class="summary-card">
                <div class="value"><%= overview != null ? overview.getOrDefault("totalGames", "-") : "-" %></div>
                <div class="label">📊 年度记录</div>
            </div>
            <div class="summary-card">
                <div class="value"><%= trendSummary != null ? trendSummary.getOrDefault("peakYear", "-") : "-" %></div>
                <div class="label">🏆 发布高峰年份</div>
            </div>
            <div class="summary-card">
                <div class="value">
                    <% if (trendSummary != null && trendSummary.get("latestYearGrowth") != null) { %>
                        <%= String.format("%.1f%%", ((Number)trendSummary.get("latestYearGrowth")).doubleValue()) %>
                    <% } else { %>-<% } %>
                </div>
                <div class="label">📈 最新同比增长率</div>
            </div>
            <div class="summary-card">
                <div class="value">
                    <% if (devSummary != null && devSummary.get("hhi") != null) { %>
                        <%= String.format("%.0f", ((Number)devSummary.get("hhi")).doubleValue()) %>
                    <% } else { %>-<% } %>
                </div>
                <div class="label">🏢 市场集中度 HHI</div>
            </div>
        </div>

        <!-- ====== 分析入口卡片 ====== -->
        <div class="section">
            <h2>📋 分析模块</h2>
            <div class="analysis-grid">
                <a href="#" onclick="loadAnalysis('trend')" class="analysis-card">
                    <div class="icon">📈</div>
                    <h3>市场趋势分析</h3>
                    <p>历年游戏发布趋势统计、拥有量变化、同比增长率（YoY）分析</p>
                </a>
                <a href="#" onclick="loadAnalysis('pricing')" class="analysis-card">
                    <div class="icon">💰</div>
                    <h3>定价与评价关联</h3>
                    <p>价格区间与用户评价关系、免费vs付费差异、高性价比游戏推荐</p>
                </a>
                <a href="#" onclick="loadAnalysis('genres')" class="analysis-card">
                    <div class="icon">🏷️</div>
                    <h3>类型标签挖掘</h3>
                    <p>游戏类型/标签频率分布、FP-Growth频繁组合挖掘、销量关联分析</p>
                </a>
                <a href="#" onclick="loadAnalysis('developers')" class="analysis-card">
                    <div class="icon">🏢</div>
                    <h3>开发商生态分析</h3>
                    <p>市场份额排名、HHI集中度指数、跨平台支持、开发商分级画像</p>
                </a>
            </div>
        </div>

        <!-- ====== 图表区域 ====== -->
        <div class="section">
            <h2>📊 可视化图表</h2>
            <div class="chart-row">
                <div class="chart-box" id="chart-trend"></div>
                <div class="chart-box" id="chart-pricing"></div>
            </div>
        </div>

        <!-- ====== 实时数据区域 ====== -->
        <div class="section">
            <h2>⚡ 实时数据仪表盘 <small style="color:#888;font-weight:normal;">（每5秒自动刷新）</small></h2>
            <div class="chart-row">
                <div class="chart-box" id="chart-realtime-games"></div>
                <div class="chart-box" id="chart-realtime-tags"></div>
            </div>
        </div>

        <!-- ====== 分析详情弹窗 ====== -->
        <div class="section" id="detail-section" style="display:none;">
            <h2 id="detail-title">分析详情</h2>
            <div id="detail-content" style="background:white;border-radius:12px;padding:20px;overflow-x:auto;"></div>
        </div>
    </main>

    <footer>
        <p>2026年第6学期 Spark 项目 | Steam Games Dataset (115,000+ games, 390MB) | 数据来源: Kaggle</p>
    </footer>

    <script>
        const ctxPath = '<%= request.getContextPath() %>';

        // ============ ECharts 图表初始化 ============

        function initChart(containerId) {
            const dom = document.getElementById(containerId);
            if (!dom) return null;
            const chart = echarts.init(dom);
            window.addEventListener('resize', () => chart.resize());
            return chart;
        }

        // 市场趋势折线图
        function renderTrendChart(data) {
            const chart = initChart('chart-trend');
            if (!chart || !data || data.length === 0) return;
            chart.setOption({
                title: { text: '游戏发布趋势', left: 'center', textStyle: { fontSize: 14 } },
                tooltip: { trigger: 'axis' },
                xAxis: { type: 'category', data: data.map(d => d.year) },
                yAxis: { type: 'value' },
                series: [
                    { name: '发布数', type: 'line', data: data.map(d => d.release_count), smooth: true,
                      itemStyle: { color: '#1a237e' } },
                    { name: '平均价格($)', type: 'line', data: data.map(d => d.avg_price), smooth: true,
                      itemStyle: { color: '#ff6f00' } }
                ],
                legend: { bottom: 0 },
                grid: { left: 50, right: 40, top: 40, bottom: 40 }
            });
        }

        // 价格分布饼图
        function renderPricingChart(data) {
            const chart = initChart('chart-pricing');
            if (!chart || !data || data.length === 0) return;
            chart.setOption({
                title: { text: '价格区间分布', left: 'center', textStyle: { fontSize: 14 } },
                tooltip: { trigger: 'item', formatter: '{b}: {c} 款 ({d}%)' },
                series: [{
                    type: 'pie',
                    radius: ['40%', '70%'],
                    data: data.map(d => ({ name: d.price_bucket, value: d.game_count })),
                    label: { formatter: '{b}\n{d}%' }
                }]
            });
        }

        // 实时游戏数据
        function renderRealtimeChart(data) {
            const chart = initChart('chart-realtime-games');
            if (!chart || !data) return;
            chart.setOption({
                title: { text: '实时新游戏上架', left: 'center', textStyle: { fontSize: 14 } },
                series: [{
                    type: 'gauge',
                    min: 0, max: 100,
                    detail: { formatter: '{value} 款' },
                    data: [{ value: data.new_games_count || 0, name: '新游戏' }]
                }]
            });
        }

        // 实时热门标签柱状图
        function renderRealtimeTagsChart(tags) {
            const chart = initChart('chart-realtime-tags');
            if (!chart || !tags || tags.length === 0) return;
            chart.setOption({
                title: { text: '实时热门标签', left: 'center', textStyle: { fontSize: 14 } },
                tooltip: { trigger: 'axis' },
                xAxis: { type: 'category', data: tags.map(t => t.tag) },
                yAxis: { type: 'value' },
                series: [{
                    type: 'bar',
                    data: tags.map(t => t.count),
                    itemStyle: { color: '#ffd54f' }
                }],
                grid: { left: 50, right: 20, top: 30, bottom: 60 },
                xAxis3D: { axisLabel: { rotate: 30 } }
            });
        }

        // ============ API 调用 ============

        async function fetchAPI(endpoint) {
            try {
                const resp = await fetch(ctxPath + '/api/analysis' + endpoint);
                return await resp.json();
            } catch (e) {
                console.error('API error:', e);
                return null;
            }
        }

        async function fetchRealtime() {
            try {
                const resp = await fetch(ctxPath + '/api/realtime/latest');
                return await resp.json();
            } catch (e) {
                console.error('Realtime API error:', e);
                return null;
            }
        }

        // ============ 加载分析详情 ============

        async function loadAnalysis(type) {
            const section = document.getElementById('detail-section');
            const title = document.getElementById('detail-title');
            const content = document.getElementById('detail-content');

            section.style.display = 'block';
            title.textContent = { trend: '📈 市场趋势分析', pricing: '💰 定价与评价分析',
                                  genres: '🏷️ 类型标签挖掘', developers: '🏢 开发商生态分析' }[type] || '分析详情';

            const result = await fetchAPI('/' + type);
            if (!result || result.status !== 'ok') {
                content.innerHTML = '<p style="color:red;">数据加载失败，请先运行 Spark 分析任务。</p>';
                return;
            }

            let html = '<table class="data-table"><thead><tr>';
            const rows = result.data || [];
            if (rows.length > 0) {
                Object.keys(rows[0]).forEach(k => { html += '<th>' + k + '</th>'; });
                html += '</tr></thead><tbody>';
                rows.forEach(row => {
                    html += '<tr>';
                    Object.values(row).forEach(v => { html += '<td>' + (v != null ? v : '-') + '</td>'; });
                    html += '</tr>';
                });
                html += '</tbody></table>';
            } else {
                html += '<tr><td>暂无数据</td></tr>';
            }
            html += '<p style="margin-top:12px;color:#888;">共 ' + rows.length + ' 条记录</p>';
            content.innerHTML = html;
            section.scrollIntoView({ behavior: 'smooth' });
        }

        // ============ 页面初始化 ============

        async function initPage() {
            // 加载市场趋势图表
            const trendData = await fetchAPI('/trend');
            if (trendData && trendData.status === 'ok') {
                renderTrendChart(trendData.data || []);
            }

            // 加载价格分布图表
            const pricingData = await fetchAPI('/pricing');
            if (pricingData && pricingData.status === 'ok') {
                renderPricingChart(pricingData.data || []);
            }

            // 加载实时数据
            const realtime = await fetchRealtime();
            if (realtime && realtime.status === 'ok') {
                renderRealtimeChart(realtime);
                renderRealtimeTagsChart(realtime.top_tags || []);
            }
        }

        // ============ 自动刷新 ============

        function startAutoRefresh() {
            setInterval(async () => {
                const realtime = await fetchRealtime();
                if (realtime && realtime.status === 'ok') {
                    renderRealtimeChart(realtime);
                    renderRealtimeTagsChart(realtime.top_tags || []);
                }
            }, 5000); // 5秒刷新
        }

        document.addEventListener('DOMContentLoaded', () => {
            initPage();
            startAutoRefresh();
        });
    </script>
</body>
</html>
