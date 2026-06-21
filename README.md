# Steam 游戏大数据分析平台

> 2026年第6学期 Spark 项目  
> 数据集: Steam Games Dataset (390MB, 115k+ games)  
> 三人小组合作项目

## 项目简介

基于 Apache Spark 的 Steam 游戏数据深度分析平台。对 Steam 平台上 11万+ 款游戏进行多维度数据挖掘，包括市场趋势、定价策略、类型关联和开发商生态分析，结果通过动态 Web 仪表盘展示。

## 总体架构

```
┌─────────────────────────────────────────┐
│           Spark 集群（Scala/SBT）         │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐   │
│  │  A1  │ │  A2  │ │  A3  │ │  A4  │   │
│  │市场趋势│ │定价评价│ │类型挖掘│ │开发商  │   │
│  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘   │
│     └─────────┼─────────┼────────┘       │
│               ▼                         │
│        MySQL (结果存储)                   │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│         Web 仪表盘（Java/Maven）          │
│     JSP/Servlet + ECharts + 积木报表     │
│     Tomcat 部署，浏览器远程访问           │
└─────────────────────────────────────────┘
```

## 技术栈

| 层次 | 技术 |
|------|------|
| 计算引擎 | Apache Spark 3.5.0 (Core / SQL / MLlib / Streaming) |
| 开发语言 | Scala 2.12 (分析) + Java 8 (Web) |
| 实时流 | Spark Structured Streaming + Kafka |
| Web 服务 | JSP / Servlet 4.0 (Jakarta) |
| 前端图表 | ECharts（交互式数据可视化）+ 积木报表 |
| 构建工具 | SBT 1.9.8 (Spark模块) + Maven (Web模块) |
| 数据存储 | HDFS (源数据) + MySQL 8.0 (分析结果) |
| 部署 | spark-submit (分析) + Tomcat (Web) |

## 项目结构

```
sem-6project/
├── build.sbt                              # Spark 模块构建配置
├── project/                               # SBT 插件配置
│
├── src/main/scala/com/spark/project/
│   ├── common/                            # 公共模块
│   │   ├── Config.scala                   #   全局配置（含 Kafka 地址）
│   │   ├── SparkSessionBuilder.scala      #   SparkSession 构建器
│   │   └── Utils.scala                    #   数据读取/清洗/存储工具
│   ├── analysis/member1/                  # 离线分析（4个）
│   │   ├── Analysis1.scala                #   A1: 市场趋势分析
│   │   ├── Analysis2.scala                #   A2: 定价与评价关联
│   │   ├── Analysis3.scala                #   A3: 类型标签挖掘 (FP-Growth)
│   │   └── Analysis4.scala                #   A4: 开发商生态 (HHI集中度)
│   ├── realtime/member1/                  # 实时流处理（加分项）
│   │   └── RealtimeStats.scala            #   Kafka → 窗口聚合 → 内存表
│   └── Main.scala                         # 主入口（串联全流程）
│
├── web/                                   # Java Web 模块（独立 Maven 项目）
│   ├── pom.xml                            #   Maven 构建（Servlet/JSP/积木报表）
│   └── src/main/
│       ├── java/com/spark/web/
│       │   ├── dao/MysqlDao.java           #   MySQL 数据访问层
│       │   └── servlet/
│       │       ├── LoginServlet.java       #   登录验证
│       │       ├── DashboardServlet.java   #   仪表盘主页
│       │       ├── AnalysisServlet.java    #   分析数据 JSON API
│       │       └── RealtimeServlet.java    #   实时数据 API（5秒轮询）
│       └── webapp/
│           ├── WEB-INF/web.xml             #   Servlet 映射配置
│           └── jsp/
│               ├── index.jsp               #   登录页
│               ├── dashboard.jsp            #   仪表盘（ECharts图表+数据表）
│               └── error.jsp               #   错误页面
│
├── scripts/
│   ├── generate_sample_data.py             #   样例数据生成脚本
│   └── kafka_producer.py                   #   Kafka 数据模拟器
│
├── data/member1/
│   ├── games.csv                           #   完整数据 (390MB, XLSX格式)
│   └── games-example.csv                   #   样例数据 (14行, 开发调试)
│
└── README.md
```

## 分析功能

### 离线批处理（4个分析模块）

| # | 分析名称 | 分析类型 | 关键指标 |
|---|----------|----------|----------|
| A1 | 市场趋势分析 | 时序聚合 + YoY增长 | 年发布数、平均拥有量、同比增长率 |
| A2 | 定价与评价关联 | 价格分桶 + 好评率 | 6个价格区间、高性价比Top-20 |
| A3 | 类型标签挖掘 | FP-Growth + 共现 | Top-50标签、频繁组合、销量关联 |
| A4 | 开发商生态 | 市场集中度 HHI | Top-50排名、跨平台、厂商分级 |

### 实时流处理（加分项）

- Kafka → Spark Structured Streaming → 滑动窗口（60s/10s）
- 前端每 5 秒自动刷新仪表盘
- Kafka 数据模拟器: `python scripts/kafka_producer.py`

## 快速开始

### 1. 准备环境

```bash
# 需要: Hadoop 3.x, Spark 3.5.0, Kafka 3.x, MySQL 8.0, Tomcat 9

# 启动 MySQL，创建数据库
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS spark_steam_games"

# 启动 Kafka（3个 broker，参考老师给的集群搭建步骤）
# 创建 Topic
bin/kafka-topics.sh --create --topic steam-game-events \
  --bootstrap-server niit-master:9091,niit-master:9092,niit-master:9093 \
  --partitions 4 --replication-factor 3
```

### 2. 运行 Spark 分析

```bash
# 编译打包
sbt assembly

# 本地测试（使用样例数据）
spark-submit --class com.spark.project.Main \
             --master local[*] \
             target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar \
             --sample

# 集群提交（使用完整数据 390MB）
spark-submit --class com.spark.project.Main \
             --master yarn \
             --deploy-mode cluster \
             --executor-memory 4G \
             --num-executors 4 \
             target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar
```

### 3. 部署 Web 仪表盘

```bash
# 编译 Web 模块
cd web
mvn clean package

# 将 target/steam-web-dashboard.war 部署到 Tomcat webapps/
# 浏览器访问: http://localhost:8080/steam-web-dashboard
```

### 4. 启动实时流（加分项）

```bash
# 终端1: 启动 Kafka 数据模拟器
python scripts/kafka_producer.py \
  --bootstrap-server niit-master:9091,niit-master:9092,niit-master:9093 \
  --topic steam-game-events --rate 5

# 终端2: 运行 Spark Streaming（在 Main.scala 中取消注释 RealtimeStats.start()）
```

## 数据流

```
games.xlsx (HDFS)
    ↓ Spark read + clean
cleaned DataFrame (cached)
    ↓ 4 parallel analysis
┌──────┬──────┬──────┬──────┐
│  A1  │  A2  │  A3  │  A4  │
└──┬───┴──┬───┴──┬───┴──┬───┘
   ↓      ↓      ↓      ↓
   └──────┴──────┴──────┘
              ↓
      MySQL (spark_steam_games)
         ├── analysis_market_trend
         ├── analysis_pricing_review
         ├── analysis_genre_tags
         └── analysis_developer_ecosystem
              ↓
      Web Dashboard (JSP/Servlet)
         ├── /api/analysis/*  (JSON API)
         └── /api/realtime/*  (实时轮询)
```

## 数据集字段

| 字段 | 类型 | 分析用途 |
|------|------|----------|
| AppID | Long | 主键 |
| Name | String | 游戏名称 |
| Release date | Date | A1: 年份趋势 |
| Estimated owners | String(范围) | A1,A2,A4: 拥有量 |
| Price | Double | A2: 定价分析 |
| Positive/Negative | Int | A2: 好评率 |
| Metacritic score | Int | A2: 评分相关性 |
| Genres | String(JSON) | A3: 类型挖掘 |
| Tags | String(JSON) | A3: 标签挖掘 |
| Developers/Publishers | String | A4: 厂商生态 |
| Windows/Mac/Linux | Bool | A4: 跨平台分析 |
| Average playtime | Long | A1: 生命周期 |
| Recommendations | Int | 热度指标 |
