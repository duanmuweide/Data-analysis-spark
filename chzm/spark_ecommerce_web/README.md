# 电商 Spark 分析结果 Web 项目

这是和 `chzm/ecommerce_analysis` 对接的 Web 可视化项目。

技术路线：

```text
Flask 后端 + MySQL 结果表 + HTML/CSS/JavaScript + ECharts
```

不需要 Tomcat、Servlet、Spring Boot、Vue、React。

## 1. 对接关系

Spark 大数据项目负责计算，Web 项目只负责展示。

```text
Spark 离线分析 -> MySQL ADS 表 -> Flask API -> ECharts
Kafka + Spark Streaming -> MySQL RT 表 -> Flask API -> ECharts 定时刷新
```

当前 Web 只查询这 5 张实际存在的结果表：

| Web 模块         | 后端接口                          | MySQL 结果表                       |
| ---------------- | --------------------------------- | ---------------------------------- |
| 销售趋势总览     | `/api/sales/trend`              | `ads_sales_time_trend`           |
| 品类销售排行     | `/api/category/top`             | `ads_category_sales_rank`        |
| 用户价值分层     | `/api/user/value-level`         | `ads_user_value_level`           |
| 品类关联规则     | `/api/category/association`     | `ads_category_association_rules` |
| 实时品类窗口销售 | `/api/realtime/category-window` | `rt_category_window_sales`       |

不要把早期方案里的 `rt_sales_window`、`rt_category_topn`、`rt_province_topn`、`rt_order_status` 写进 Web，因为当前 Spark 代码和 SQL 没有生成这些表。

## 2. 用 VSCode 打开

直接用 VSCode 打开这个目录：

```text
D:\NIIT_Project\Data-analysis-spark\chzm\spark_ecommerce_web
```

建议安装 VSCode 插件：

```text
Python
Pylance
```

## 3. 本地 Python 环境配置

在 VSCode 终端里执行：

```powershell
cd D:\NIIT_Project\Data-analysis-spark\chzm\spark_ecommerce_web
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

如果 PowerShell 不允许激活虚拟环境，可以临时执行：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\activate
```

或者不用激活，直接这样执行：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

## 4. 配置数据库连接

复制配置模板：

```powershell
copy .env.example .env
```

如果 Web 和 MySQL 都在你自己的 Windows 电脑上运行，`.env` 推荐这样写：

```text
WEB_HOST=127.0.0.1
WEB_PORT=5000
FLASK_DEBUG=true

DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=spark
DB_PASSWORD=spark123456
DB_NAME=spark_ecommerce
DB_CHARSET=utf8mb4

REALTIME_REFRESH_SECONDS=5
```

注意：

Spark 在虚拟机里写 Windows MySQL 时，`spark-submit --jdbc-url` 要用虚拟机能访问到的 Windows IP，例如 `192.168.211.1`。

但是 Flask Web 如果也运行在 Windows 本机，并且 MySQL 也在 Windows 本机，`.env` 里可以写 `DB_HOST=127.0.0.1`。

## 5. 本地启动 Web

确保 MySQL 已经执行过 `mysql_init.sql`，并且至少 5 张结果表已经建好。

启动：

```powershell
python app.py
```

或者：

```powershell
.\.venv\Scripts\python.exe app.py
```

浏览器访问：

```text
http://127.0.0.1:5000
```

检查接口：

```text
http://127.0.0.1:5000/api/health
http://127.0.0.1:5000/api/sales/trend
http://127.0.0.1:5000/api/category/top
http://127.0.0.1:5000/api/user/value-level
http://127.0.0.1:5000/api/category/association
http://127.0.0.1:5000/api/realtime/category-window
```

## 6. 正式跑项目的顺序

建议从头执行时按这个顺序：

1. Windows 或服务器 MySQL 执行 `chzm/ecommerce_analysis/scripts/mysql_init.sql`。
2. 上传 `users.csv`、`products.csv`、`orders.csv`、`order_items.csv` 到 HDFS。
3. 打胖包并上传到虚拟机。
4. 先执行 `OfflineJob --dry-run`，确认 4 个离线分析能跑出来。
5. 正式执行 `OfflineJob` 写入 MySQL。
6. 启动 Flask Web，确认 4 个离线图表能显示。
7. 启动 Kafka。
8. 启动 `StreamingJob`。
9. 启动 `kafka_order_producer.py`。
10. 打开 Web 页面，看实时图表自动刷新。

实时任务的关键顺序：

```text
先启动 StreamingJob，再启动 producer。
```

原因是当前 `StreamingJob.scala` 使用：

```text
startingOffsets=latest
```

它只消费 StreamingJob 启动之后新进入 Kafka 的消息。

## 7. 服务器部署

Linux 服务器上推荐使用：

```text
Flask + Gunicorn + Nginx + MySQL
```

部署命令示例：

```bash
cd /opt/czm/spark_ecommerce_web
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
vi .env
gunicorn -w 2 -b 0.0.0.0:8000 app:app
```

访问：

```text
http://服务器IP:8000
```

如果配置 Nginx，可以把 80 端口反向代理到 8000 端口。

## 8. ECharts 说明

页面优先加载本地文件：

```text
static/vendor/echarts.min.js
```

如果本地文件不存在，会尝试从 CDN 加载：

```text
https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js
```

答辩环境如果没有外网，必须提前保证 `static/vendor/echarts.min.js` 存在。

## 9. 常见问题

1. 页面能打开，但没有图。

   - 先访问 `/api/health`。
   - 检查 MySQL 是否启动。
   - 检查 `.env` 的数据库账号密码。
   - 检查 5 张结果表是否已经创建。
2. 离线图没有数据。

   - 说明 4 张 ADS 表还没有 Spark 写入结果。
   - 先跑 `OfflineJob`。
3. 实时图没有数据。

   - 说明 `rt_category_window_sales` 还没有实时结果。
   - 启动顺序必须是 Web -> StreamingJob -> producer。
4. Spark 能写 MySQL，但 Web 查不到。

   - Spark 的 JDBC 地址和 Flask 的 DB_HOST 可以不同。
   - 如果 MySQL 和 Flask 在同一台 Windows 上，Flask 用 `127.0.0.1`。
   - 如果 Flask 在服务器上，DB_HOST 要写服务器能访问的 MySQL 地址。
