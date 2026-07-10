<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>房屋价格分析 Dashboard</title>
    <link rel="stylesheet" href="css/style.css">
    <!-- ECharts CDN -->
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
</head>
<body>

<!-- ==================== 顶部导航 ==================== -->
<div class="header">
    <div>
        <h1>🏠 房屋价格分析平台</h1>
        <span class="subtitle">Spark SQL 离线分析 + Kafka 实时流分析</span>
    </div>
    <div style="text-align:right;">
        <span id="currentTime" style="color:#8b949e;font-size:13px;"></span>
    </div>
</div>

<!-- ==================== Dashboard 网格 ==================== -->
<div class="dashboard">

    <!-- 1. 面积区间房价分析（左上） -->
    <div class="chart-panel">
        <div class="panel-header">
            <h2>📐 面积区间房价分析</h2>
            <span class="badge">Spark 离线</span>
        </div>
        <div class="chart-container" id="chart-area"></div>
    </div>

    <!-- 2. 市区房价对比（右上） -->
    <div class="chart-panel">
        <div class="panel-header">
            <h2>🏙️ 市区房价对比</h2>
            <span class="badge">Spark 离线</span>
        </div>
        <div class="chart-container" id="chart-district"></div>
    </div>

    <!-- 3. 年份区间房屋分析（中左） -->
    <div class="chart-panel">
        <div class="panel-header">
            <h2>📅 年份区间户型分布</h2>
            <span class="badge">Spark 离线</span>
        </div>
        <div class="chart-container" id="chart-year"></div>
    </div>

    <!-- 4. 小区房价 Top20（中右） -->
    <div class="chart-panel">
        <div class="panel-header">
            <h2>🏘️ 小区房价 Top20</h2>
            <span class="badge">Spark 离线</span>
        </div>
        <div class="chart-container" id="chart-community"></div>
    </div>

    <!-- 5. 实时小区分析（底部全宽） -->
    <div class="chart-panel full-width realtime">
        <div class="panel-header">
            <h2>
                🔴 海淀区 90-144㎡ 小区实时均价
                <span class="live-indicator">
                    <span class="live-dot"></span> 实时更新中
                </span>
            </h2>
            <span class="badge">Kafka 实时流</span>
        </div>
        <div class="chart-container" id="chart-realtime"></div>
    </div>

</div>

<!-- ==================== 底部 ==================== -->
<div class="footer">
    <span>数据来源：MySQL cjz_spark 数据库</span> &nbsp;|&nbsp;
    <span class="update-time" id="lastUpdateTime">--</span>
</div>

<!-- ==================== 核心脚本 ==================== -->
<script>
// ============================================================
// 通用工具
// ============================================================
var BASE_PATH = '<%= request.getContextPath() %>';

function fetchData(table) {
    return fetch(BASE_PATH + '/data?table=' + table)
        .then(function(res) { return res.json(); })
        .then(function(json) {
            if (!json.success) {
                console.error('数据加载失败 [' + table + ']:', json.error);
                return [];
            }
            return json.data;
        })
        .catch(function(err) {
            console.error('网络请求失败 [' + table + ']:', err);
            return [];
        });
}

// 深色主题 ECharts 通用配置
function darkThemeOptions() {
    return {
        backgroundColor: 'transparent',
        textStyle: { color: '#8b949e' },
        legend: { textStyle: { color: '#8b949e' } },
        tooltip: {
            backgroundColor: 'rgba(13,17,23,0.95)',
            borderColor: '#30363d',
            textStyle: { color: '#e6edf3' }
        }
    };
}

// 更新时钟
function updateClock() {
    var now = new Date();
    document.getElementById('currentTime').textContent =
        now.toLocaleString('zh-CN', { hour12: false });
    document.getElementById('lastUpdateTime').textContent =
        '最后刷新：' + now.toLocaleTimeString('zh-CN');
}
updateClock();
setInterval(updateClock, 1000);

// ============================================================
// 图表实例
// ============================================================
var chartArea      = echarts.init(document.getElementById('chart-area'));
var chartDistrict  = echarts.init(document.getElementById('chart-district'));
var chartYear      = echarts.init(document.getElementById('chart-year'));
var chartCommunity = echarts.init(document.getElementById('chart-community'));
var chartRealtime  = echarts.init(document.getElementById('chart-realtime'));

// 响应窗口缩放
window.addEventListener('resize', function() {
    chartArea.resize();
    chartDistrict.resize();
    chartYear.resize();
    chartCommunity.resize();
    chartRealtime.resize();
});

// ============================================================
// 1. 面积区间房价分析
// ============================================================
function renderAreaChart(data) {
    // 按 area_range + price_level 分组
    var ranges = ['50-90㎡', '90-144㎡', '144-236㎡', '236㎡以上'];
    var levels = ['低价位', '中等价位', '中高价位', '高价位'];
    var levelColors = {
        '低价位':   '#3fb950',
        '中等价位': '#58a6ff',
        '中高价位': '#d29922',
        '高价位':   '#f85149'
    };

    // 构建数据：每个 price_level 一个 series
    var series = [];
    levels.forEach(function(level) {
        var seriesData = ranges.map(function(range) {
            var found = null;
            for (var i = 0; i < data.length; i++) {
                if (data[i].area_range === range && data[i].price_level === level) {
                    found = data[i];
                    break;
                }
            }
            return found ? found.avg_price_per_sqm : 0;
        });
        series.push({
            name: level,
            type: 'bar',
            barWidth: '18%',
            data: seriesData,
            itemStyle: {
                color: levelColors[level],
                borderRadius: [4, 4, 0, 0]
            },
            label: {
                show: true,
                position: 'top',
                fontSize: 10,
                color: '#8b949e',
                formatter: function(p) {
                    return p.value > 0 ? (p.value / 1000).toFixed(1) + 'k' : '';
                }
            }
        });
    });

    chartArea.setOption({
        ...darkThemeOptions(),
        title: {
            text: '各面积区间均价对比（按价格等级分层）',
            textStyle: { color: '#e6edf3', fontSize: 13 },
            left: 'center',
            top: 5
        },
        tooltip: {
            ...darkThemeOptions().tooltip,
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: function(params) {
                var s = '<b>' + params[0].axisValue + '</b><br/>';
                params.forEach(function(p) {
                    if (p.value > 0) {
                        s += p.marker + ' ' + p.seriesName + '：' +
                             p.value.toLocaleString() + ' 元/㎡<br/>';
                    }
                });
                return s;
            }
        },
        legend: {
            data: levels,
            bottom: 5,
            textStyle: { color: '#8b949e', fontSize: 11 }
        },
        grid: { left: '8%', right: '8%', top: '14%', bottom: '12%' },
        xAxis: {
            type: 'category',
            data: ranges,
            axisLabel: { color: '#8b949e', fontSize: 12 },
            axisLine: { lineStyle: { color: '#21262d' } },
            axisTick: { show: false }
        },
        yAxis: {
            type: 'value',
            name: '元/㎡',
            axisLabel: {
                color: '#8b949e',
                formatter: function(v) { return (v / 1000).toFixed(0) + 'k'; }
            },
            splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } }
        },
        series: series
    });
}

// ============================================================
// 2. 市区房价对比
// ============================================================
function renderDistrictChart(data) {
    // 按 district 聚合（取最新 checkid 的数据）
    var districts = [];
    var avgPrices = [];
    var houseCounts = [];

    data.forEach(function(row) {
        districts.push(row.district);
        avgPrices.push(row.avg_price_per_sqm);
        houseCounts.push(row.house_count);
    });

    chartDistrict.setOption({
        ...darkThemeOptions(),
        title: {
            text: '各市区平均房价与房屋数量',
            textStyle: { color: '#e6edf3', fontSize: 13 },
            left: 'center',
            top: 5
        },
        tooltip: {
            ...darkThemeOptions().tooltip,
            trigger: 'axis',
            axisPointer: { type: 'cross' }
        },
        legend: {
            data: ['平均房价', '房屋数量'],
            bottom: 5,
            textStyle: { color: '#8b949e', fontSize: 11 }
        },
        grid: { left: '8%', right: '8%', top: '14%', bottom: '12%' },
        xAxis: {
            type: 'category',
            data: districts,
            axisLabel: { color: '#8b949e', fontSize: 11, rotate: 30 },
            axisLine: { lineStyle: { color: '#21262d' } }
        },
        yAxis: [
            {
                type: 'value',
                name: '元/㎡',
                axisLabel: {
                    color: '#58a6ff',
                    formatter: function(v) { return (v / 1000).toFixed(0) + 'k'; }
                },
                splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } }
            },
            {
                type: 'value',
                name: '套数',
                axisLabel: { color: '#3fb950' },
                splitLine: { show: false }
            }
        ],
        series: [
            {
                name: '平均房价',
                type: 'bar',
                data: avgPrices,
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: '#58a6ff' },
                        { offset: 1, color: '#1f6feb' }
                    ]),
                    borderRadius: [4, 4, 0, 0]
                },
                barWidth: '50%'
            },
            {
                name: '房屋数量',
                type: 'line',
                yAxisIndex: 1,
                data: houseCounts,
                lineStyle: { color: '#3fb950', width: 2 },
                itemStyle: { color: '#3fb950' },
                symbol: 'circle',
                symbolSize: 8
            }
        ]
    });
}

// ============================================================
// 3. 年份区间户型分布（堆叠柱状图）
// ============================================================
function renderYearChart(data) {
    var yearRanges = [];
    var smallData = [], mediumData = [], largeData = [];

    data.forEach(function(row) {
        yearRanges.push(row.year_range);
        smallData.push(row.small_layout_count);
        mediumData.push(row.medium_layout_count);
        largeData.push(row.large_layout_count);
    });

    chartYear.setOption({
        ...darkThemeOptions(),
        title: {
            text: '各年份区间户型分布（小/中/大户型）',
            textStyle: { color: '#e6edf3', fontSize: 13 },
            left: 'center',
            top: 5
        },
        tooltip: {
            ...darkThemeOptions().tooltip,
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: function(params) {
                var s = '<b>' + params[0].axisValue + '</b><br/>';
                var total = 0;
                params.forEach(function(p) { total += p.value; });
                params.forEach(function(p) {
                    var pct = total > 0 ? (p.value / total * 100).toFixed(1) : 0;
                    s += p.marker + ' ' + p.seriesName + '：' +
                         p.value.toLocaleString() + ' 套（' + pct + '%）<br/>';
                });
                return s;
            }
        },
        legend: {
            data: ['小户型(1-2室)', '中户型(3-4室)', '大户型(5室+)'],
            bottom: 5,
            textStyle: { color: '#8b949e', fontSize: 11 }
        },
        grid: { left: '6%', right: '6%', top: '14%', bottom: '12%' },
        xAxis: {
            type: 'category',
            data: yearRanges,
            axisLabel: { color: '#8b949e', fontSize: 12 },
            axisLine: { lineStyle: { color: '#21262d' } }
        },
        yAxis: {
            type: 'value',
            name: '套',
            axisLabel: { color: '#8b949e' },
            splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } }
        },
        series: [
            {
                name: '小户型(1-2室)',
                type: 'bar',
                stack: 'total',
                data: smallData,
                itemStyle: { color: '#58a6ff' },
                emphasis: { focus: 'series' }
            },
            {
                name: '中户型(3-4室)',
                type: 'bar',
                stack: 'total',
                data: mediumData,
                itemStyle: { color: '#d29922' },
                emphasis: { focus: 'series' }
            },
            {
                name: '大户型(5室+)',
                type: 'bar',
                stack: 'total',
                data: largeData,
                itemStyle: { color: '#f85149' },
                emphasis: { focus: 'series' }
            }
        ]
    });
}

// ============================================================
// 4. 小区房价 Top20（横向柱状图）
// ============================================================
function renderCommunityChart(data) {
    // 按均价降序排序，取 Top20
    var sorted = data.slice().sort(function(a, b) {
        return b.avg_price_per_sqm - a.avg_price_per_sqm;
    });
    var top20 = sorted.slice(0, 20);

    // 横向柱状图：Y轴显示小区名（反转）
    var names = top20.map(function(r) { return r.community; }).reverse();
    var prices = top20.map(function(r) { return r.avg_price_per_sqm; }).reverse();
    var counts = top20.map(function(r) { return r.house_count; }).reverse();

    chartCommunity.setOption({
        ...darkThemeOptions(),
        title: {
            text: '均价最高小区 Top20',
            textStyle: { color: '#e6edf3', fontSize: 13 },
            left: 'center',
            top: 5
        },
        tooltip: {
            ...darkThemeOptions().tooltip,
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: function(params) {
                var idx = params[0].dataIndex;
                var realIdx = top20.length - 1 - idx;
                return '<b>' + top20[realIdx].district + ' - ' + params[0].name + '</b><br/>' +
                       params[0].marker + ' 均价：' + params[0].value.toLocaleString() + ' 元/㎡<br/>' +
                       '房屋数量：' + top20[realIdx].house_count + ' 套';
            }
        },
        grid: { left: '3%', right: '12%', top: '10%', bottom: '4%', containLabel: true },
        xAxis: {
            type: 'value',
            name: '元/㎡',
            axisLabel: {
                color: '#8b949e',
                formatter: function(v) { return (v / 1000).toFixed(0) + 'k'; }
            },
            splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } }
        },
        yAxis: {
            type: 'category',
            data: names,
            axisLabel: {
                color: '#e6edf3',
                fontSize: 11,
                width: 80,
                overflow: 'truncate'
            },
            axisLine: { lineStyle: { color: '#21262d' } }
        },
        series: [{
            type: 'bar',
            data: prices.map(function(v, i) {
                return {
                    value: v,
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                            { offset: 0, color: '#1f6feb' },
                            { offset: 1, color: '#58a6ff' }
                        ]),
                        borderRadius: [0, 4, 4, 0]
                    }
                };
            }),
            barWidth: '60%',
            label: {
                show: true,
                position: 'right',
                fontSize: 10,
                color: '#8b949e',
                formatter: function(p) {
                    return (p.value / 1000).toFixed(1) + 'k';
                }
            }
        }]
    });
}

// ============================================================
// 5. 实时小区分析（AJAX 轮询，每 3 秒刷新）
// ============================================================
function renderRealtimeChart(data) {
    // 按均价降序排列
    var sorted = data.slice().sort(function(a, b) {
        return b.averageprice - a.averageprice;
    });

    var names = sorted.map(function(r) { return r.community; }).reverse();
    var prices = sorted.map(function(r) { return r.averageprice; }).reverse();
    var numbers = sorted.map(function(r) { return r.number; }).reverse();

    chartRealtime.setOption({
        ...darkThemeOptions(),
        title: {
            text: '海淀区 90-144㎡ 小区实时均价（共 ' + data.length + ' 个小区）',
            textStyle: { color: '#e6edf3', fontSize: 13 },
            left: 'center',
            top: 5
        },
        tooltip: {
            ...darkThemeOptions().tooltip,
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            formatter: function(params) {
                var idx = params[0].dataIndex;
                var realIdx = sorted.length - 1 - idx;
                return '<b>' + sorted[realIdx].community + '</b><br/>' +
                       params[0].marker + ' 均价：' + params[0].value.toLocaleString() + ' 元/㎡<br/>' +
                       '房源数量：' + sorted[realIdx].number + ' 套';
            }
        },
        grid: { left: '3%', right: '12%', top: '10%', bottom: '4%', containLabel: true },
        xAxis: {
            type: 'value',
            name: '元/㎡',
            axisLabel: {
                color: '#8b949e',
                formatter: function(v) { return (v / 1000).toFixed(0) + 'k'; }
            },
            splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } }
        },
        yAxis: {
            type: 'category',
            data: names,
            axisLabel: {
                color: '#e6edf3',
                fontSize: 11,
                width: 100,
                overflow: 'truncate'
            },
            axisLine: { lineStyle: { color: '#21262d' } }
        },
        series: [{
            type: 'bar',
            data: prices.map(function(v, i) {
                return {
                    value: v,
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                            { offset: 0, color: '#238636' },
                            { offset: 1, color: '#3fb950' }
                        ]),
                        borderRadius: [0, 4, 4, 0]
                    }
                };
            }),
            barWidth: '60%',
            label: {
                show: true,
                position: 'right',
                fontSize: 10,
                color: '#3fb950',
                formatter: function(p) {
                    return (p.value / 1000).toFixed(1) + 'k';
                }
            }
        }]
    }, true); // notMerge=true，每次完全替换，确保实时刷新
}

// 轮询实时数据
function pollRealtime() {
    fetchData('certain').then(function(data) {
        if (data && data.length > 0) {
            renderRealtimeChart(data);
            document.getElementById('lastUpdateTime').textContent =
                '最后刷新：' + new Date().toLocaleTimeString('zh-CN');
        }
    });
}

// ============================================================
// 启动：加载所有数据
// ============================================================
window.addEventListener('DOMContentLoaded', function() {
    // 并行加载 4 个离线分析表
    Promise.all([
        fetchData('area'),
        fetchData('district'),
        fetchData('year'),
        fetchData('community')
    ]).then(function(results) {
        var areaData      = results[0];
        var districtData  = results[1];
        var yearData      = results[2];
        var communityData = results[3];

        if (areaData.length > 0)      renderAreaChart(areaData);
        if (districtData.length > 0)  renderDistrictChart(districtData);
        if (yearData.length > 0)      renderYearChart(yearData);
        if (communityData.length > 0) renderCommunityChart(communityData);

        console.log('========== 离线分析图表加载完成 ==========');
        console.log('面积分析数据:', areaData.length, '条');
        console.log('市区分析数据:', districtData.length, '条');
        console.log('年份分析数据:', yearData.length, '条');
        console.log('小区分析数据:', communityData.length, '条');
    }).catch(function(err) {
        console.error('图表初始化失败:', err);
    });

    // 启动实时轮询
    pollRealtime();
    setInterval(pollRealtime, 3000); // 每 3 秒刷新

    console.log('========== 实时轮询已启动（每 3 秒） ==========');
});
</script>

</body>
</html>
