const charts = {};
const levelOrder = ["高价值用户", "重点发展用户", "一般保持用户", "低价值用户"];
const memberOrder = ["普通", "白银", "黄金", "铂金", "黑金", "未知"];

function $(id) {
  return document.getElementById(id);
}

function setMessage(message) {
  const el = $("global-message");
  if (!el) return;
  if (!message) {
    el.hidden = true;
    el.textContent = "";
    return;
  }
  el.hidden = false;
  el.textContent = message;
}

function setHealth(ok, message) {
  const status = document.querySelector(".status");
  const text = $("health-text");
  if (!status || !text) return;
  status.classList.toggle("ok", ok);
  status.classList.toggle("error", !ok);
  text.textContent = message;
}

function formatNumber(value, digits = 0) {
  const number = Number(value || 0);
  return number.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatCurrency(value) {
  const number = Number(value || 0);
  return number.toLocaleString("zh-CN", {
    style: "currency",
    currency: "CNY",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
}

function formatPercent(value) {
  return `${formatNumber(Number(value || 0) * 100, 2)}%`;
}

async function fetchApi(path) {
  const response = await fetch(path, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok || !payload.success) {
    throw new Error(payload.message || `接口请求失败：${path}`);
  }
  return payload.data;
}

function initCharts() {
  if (!window.echarts) {
    setMessage("ECharts 加载失败。请检查网络，或把 echarts.min.js 放到 static/vendor 目录。");
    return false;
  }

  [
    "sales-daily-chart",
    "sales-hourly-chart",
    "category-top-chart",
    "user-level-chart",
    "member-level-chart",
    "association-graph",
    "realtime-category-chart",
    "realtime-trend-chart"
  ].forEach((id) => {
    charts[id] = echarts.init($(id));
  });

  window.addEventListener("resize", () => {
    Object.values(charts).forEach((chart) => chart.resize());
  });

  return true;
}

function emptyOption(text) {
  return {
    title: {
      text,
      left: "center",
      top: "middle",
      textStyle: { color: "#667589", fontSize: 14, fontWeight: 400 }
    },
    xAxis: { show: false },
    yAxis: { show: false },
    series: []
  };
}

function renderTable(tbodyId, rows, columns) {
  const tbody = $(tbodyId);
  tbody.innerHTML = "";

  if (!rows || rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.className = "empty-row";
    td.colSpan = columns.length;
    td.textContent = "暂无数据";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  rows.forEach((row, index) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      td.textContent = column.render ? column.render(row, index) : row[column.key];
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

async function loadHealth() {
  try {
    const data = await fetchApi("/api/health");
    const totalRows = data.tables.reduce((sum, item) => sum + Number(item.row_count || 0), 0);
    setHealth(data.database_ok, data.database_ok ? `MySQL 已连接，结果行数 ${formatNumber(totalRows)}` : "MySQL 未连接");
  } catch (error) {
    setHealth(false, "MySQL 连接失败");
    throw error;
  }
}

async function loadSalesTrend() {
  const data = await fetchApi("/api/sales/trend");
  const summary = data.summary || {};
  const daily = data.daily || [];
  const hourly = data.hourly || [];

  $("metric-pay-amount").textContent = formatCurrency(summary.pay_amount);
  $("metric-order-count").textContent = formatNumber(summary.order_count);
  $("metric-avg-order").textContent = formatCurrency(summary.avg_order_amount);
  $("metric-last-update").textContent = summary.last_update || "--";

  if (daily.length === 0) {
    charts["sales-daily-chart"].setOption(emptyOption("暂无销售趋势数据"), true);
  } else {
    charts["sales-daily-chart"].setOption({
      tooltip: { trigger: "axis" },
      legend: { top: 0, data: ["实付金额", "订单数", "客单价"] },
      grid: { top: 48, left: 56, right: 54, bottom: 48 },
      xAxis: { type: "category", data: daily.map((item) => item.stat_date), axisLabel: { rotate: 35 } },
      yAxis: [
        { type: "value", name: "金额", axisLabel: { formatter: (value) => `${value}` } },
        { type: "value", name: "订单数" }
      ],
      series: [
        {
          name: "实付金额",
          type: "line",
          smooth: true,
          areaStyle: { opacity: 0.12 },
          data: daily.map((item) => item.pay_amount)
        },
        {
          name: "订单数",
          type: "bar",
          yAxisIndex: 1,
          data: daily.map((item) => item.order_count)
        },
        {
          name: "客单价",
          type: "line",
          smooth: true,
          data: daily.map((item) => item.avg_order_amount)
        }
      ]
    }, true);
  }

  if (hourly.length === 0) {
    charts["sales-hourly-chart"].setOption(emptyOption("暂无小时数据"), true);
  } else {
    charts["sales-hourly-chart"].setOption({
      tooltip: { trigger: "axis" },
      grid: { top: 28, left: 52, right: 24, bottom: 42 },
      xAxis: { type: "category", data: hourly.map((item) => `${item.stat_hour_label}:00`) },
      yAxis: { type: "value", name: "订单数" },
      series: [{
        name: "订单数",
        type: "bar",
        barMaxWidth: 28,
        data: hourly.map((item) => item.order_count)
      }]
    }, true);
  }
}

async function loadCategoryTop() {
  const data = await fetchApi("/api/category/top?limit=10");
  const items = data.items || [];
  const chartItems = [...items].reverse();

  if (items.length === 0) {
    charts["category-top-chart"].setOption(emptyOption("暂无品类排行数据"), true);
  } else {
    charts["category-top-chart"].setOption({
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { top: 20, left: 86, right: 24, bottom: 34 },
      xAxis: { type: "value", name: "销售额" },
      yAxis: { type: "category", data: chartItems.map((item) => item.category) },
      series: [{
        name: "销售额",
        type: "bar",
        data: chartItems.map((item) => item.amount_sum)
      }]
    }, true);
  }

  renderTable("category-top-table", items, [
    { render: (row, index) => index + 1 },
    { key: "category" },
    { render: (row) => formatCurrency(row.amount_sum) },
    { render: (row) => formatNumber(row.quantity_sum) },
    { render: (row) => formatNumber(row.order_count) }
  ]);
}

async function loadUserValueLevel() {
  const data = await fetchApi("/api/user/value-level");
  const levels = data.levels || [];
  const memberLevels = data.member_levels || [];

  if (levels.length === 0) {
    charts["user-level-chart"].setOption(emptyOption("暂无用户分层数据"), true);
  } else {
    charts["user-level-chart"].setOption({
      tooltip: { trigger: "item", formatter: "{b}<br/>人数：{c}<br/>占比：{d}%" },
      legend: { bottom: 0 },
      series: [{
        name: "用户层级",
        type: "pie",
        radius: ["45%", "70%"],
        center: ["50%", "45%"],
        data: levels.map((item) => ({ name: item.user_level, value: item.user_count }))
      }]
    }, true);
  }

  const members = Array.from(new Set(memberLevels.map((item) => item.member_level)))
    .sort((a, b) => memberOrder.indexOf(a) - memberOrder.indexOf(b));
  const levelNames = levelOrder.filter((level) => memberLevels.some((item) => item.user_level === level));

  if (memberLevels.length === 0) {
    charts["member-level-chart"].setOption(emptyOption("暂无会员分布数据"), true);
  } else {
    charts["member-level-chart"].setOption({
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { top: 0 },
      grid: { top: 48, left: 54, right: 24, bottom: 42 },
      xAxis: { type: "category", data: members },
      yAxis: { type: "value", name: "人数" },
      series: levelNames.map((level) => ({
        name: level,
        type: "bar",
        stack: "total",
        data: members.map((member) => {
          const row = memberLevels.find((item) => item.member_level === member && item.user_level === level);
          return row ? row.user_count : 0;
        })
      }))
    }, true);
  }
}

async function loadAssociationRules() {
  const data = await fetchApi("/api/category/association?limit=10");
  const rules = data.rules || [];
  const graph = data.graph || { nodes: [], links: [] };

  renderTable("association-table", rules, [
    { key: "antecedent" },
    { key: "consequent" },
    { render: (row) => formatPercent(row.support) },
    { render: (row) => formatPercent(row.confidence) },
    { render: (row) => formatNumber(row.lift, 3) }
  ]);

  if (graph.nodes.length === 0) {
    charts["association-graph"].setOption(emptyOption("暂无关联规则数据"), true);
  } else {
    charts["association-graph"].setOption({
      tooltip: {
        formatter: (params) => {
          if (params.dataType === "edge") {
            return `${params.data.source} -> ${params.data.target}<br/>lift：${formatNumber(params.data.lift, 3)}<br/>confidence：${formatPercent(params.data.confidence)}`;
          }
          return params.name;
        }
      },
      series: [{
        type: "graph",
        layout: "force",
        roam: true,
        draggable: true,
        symbolSize: 46,
        label: { show: true },
        force: { repulsion: 180, edgeLength: 110 },
        edgeSymbol: ["none", "arrow"],
        edgeSymbolSize: 8,
        data: graph.nodes,
        links: graph.links,
        lineStyle: { width: 2, opacity: 0.75, curveness: 0.15 }
      }]
    }, true);
  }
}

async function loadRealtime() {
  const data = await fetchApi("/api/realtime/category-window?limit=20");
  const latest = data.latest || [];
  const trend = data.trend || [];
  const recent = data.recent || [];
  const chartItems = [...latest].reverse();

  if (latest.length === 0) {
    charts["realtime-category-chart"].setOption(emptyOption("暂无实时窗口数据"), true);
  } else {
    charts["realtime-category-chart"].setOption({
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { top: 24, left: 84, right: 24, bottom: 38 },
      xAxis: { type: "value", name: "销售额" },
      yAxis: { type: "category", data: chartItems.map((item) => item.category) },
      series: [{
        name: "销售额",
        type: "bar",
        data: chartItems.map((item) => item.pay_amount)
      }]
    }, true);
  }

  if (trend.length === 0) {
    charts["realtime-trend-chart"].setOption(emptyOption("暂无实时趋势数据"), true);
  } else {
    charts["realtime-trend-chart"].setOption({
      tooltip: { trigger: "axis" },
      legend: { top: 0, data: ["销售额", "订单数"] },
      grid: { top: 48, left: 54, right: 48, bottom: 54 },
      xAxis: { type: "category", data: trend.map((item) => item.window_end), axisLabel: { rotate: 30 } },
      yAxis: [
        { type: "value", name: "销售额" },
        { type: "value", name: "订单数" }
      ],
      series: [
        { name: "销售额", type: "line", smooth: true, data: trend.map((item) => item.pay_amount) },
        { name: "订单数", type: "bar", yAxisIndex: 1, data: trend.map((item) => item.order_count) }
      ]
    }, true);
  }

  renderTable("realtime-table", recent, [
    { key: "window_start" },
    { key: "window_end" },
    { key: "category" },
    { render: (row) => formatNumber(row.order_count) },
    { render: (row) => formatCurrency(row.pay_amount) },
    { key: "batch_id" },
    { key: "update_time" }
  ]);
}

async function loadOfflineModules() {
  const tasks = [
    loadHealth(),
    loadSalesTrend(),
    loadCategoryTop(),
    loadUserValueLevel(),
    loadAssociationRules()
  ];
  const results = await Promise.allSettled(tasks);
  const rejected = results.find((item) => item.status === "rejected");
  if (rejected) {
    setMessage(rejected.reason.message);
  } else {
    setMessage("");
  }
}

async function start() {
  if (!initCharts()) return;

  await loadOfflineModules();

  try {
    await loadRealtime();
  } catch (error) {
    setMessage(error.message);
  }

  const refreshSeconds = Number(window.DASHBOARD_CONFIG?.refreshSeconds || 5);
  window.setInterval(async () => {
    try {
      await loadRealtime();
      await loadHealth();
    } catch (error) {
      setMessage(error.message);
    }
  }, Math.max(refreshSeconds, 3) * 1000);
}

document.addEventListener("DOMContentLoaded", start);
