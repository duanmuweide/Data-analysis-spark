# Steam 游戏大数据分析平台

> 2026年第6学期 Spark 项目
> 数据集: Steam Games Dataset (390MB, 115k+ games)

## 项目简介

基于 Apache Spark 的 Steam 游戏数据深度分析平台。对 Steam 平台上 11万+ 款游戏进行多维度数据挖掘，包括市场趋势、定价策略、类型关联和开发商生态分析，结果通过动态 Web 仪表盘展示。

## 技术栈

| 层次 | 技术 |
|------|------|
| 计算引擎 | Apache Spark 3.5.0 (Core / SQL / MLlib / Streaming) |
| 开发语言 | Scala 2.12 |
| Web 服务 | Akka HTTP + 动态模板 |
| 前端图表 | ECharts（交互式数据可视化） |
| 构建工具 | SBT 1.9.8 |
| 数据存储 | HDFS (源数据) + MySQL (结果) |

## 项目结构

```
sem-6project/
├── build.sbt                     # 项目构建配置
├── project/                      # SBT 配置
├── src/main/scala/
│   └── com/spark/project/
│       ├── common/               # 公共模块
│       │   ├── SparkSessionBuilder.scala
│       │   ├── Config.scala      # 全局配置
│       │   └── Utils.scala       # 工具类（Steam数据清洗）
│       ├── analysis/member1/     # 离线分析模块（4个）
│       │   ├── Analysis1.scala   #   市场趋势分析
│       │   ├── Analysis2.scala   #   定价与评价关联
│       │   ├── Analysis3.scala   #   类型标签关联挖掘
│       │   └── Analysis4.scala   #   开发商生态分析
│       ├── realtime/member1/     # 实时流处理（加分项）
│       │   └── RealtimeStats.scala
│       ├── web/                  # Web 服务
│       │   ├── WebServer.scala
│       │   └── ApiRoutes.scala
│       └── Main.scala            # 主入口
├── data/member1/                 # Steam 游戏数据
│   ├── games.csv                 #   完整数据 (390MB, 115k+ 行)
│   └── games-example.csv         #   样例数据 (14行, 开发调试用)
├── scripts/                      # 辅助脚本
└── README.md
```

## 数据集字段说明 (Steam Games)

| 字段 | 类型 | 说明 | 分析用途 |
|------|------|------|----------|
| AppID | Long | Steam 应用唯一ID | 主键 |
| Name | String | 游戏名称 | - |
| Release date | Date | 发售日期 | A1: 年份趋势 |
| Estimated owners | String | 估算拥有量范围 | A1,A2,A4: 销量/份额 |
| Peak CCU | Int | 最高同时在线 | 辅助指标 |
| Price | Double | 售价(USD) | A2: 定价分析 |
| Discount | Int | 折扣百分比 | 辅助 |
| Positive / Negative | Int | 好评/差评数 | A2: 好评率 |
| Metacritic score | Int | 专业评分 | A2: 评分相关性 |
| Categories | String | 功能分类(JSON数组) | A3: 分类分析 |
| Genres | String | 游戏类型(JSON数组) | A3: 类型挖掘 |
| Tags | String | 用户标签(JSON数组) | A3: 标签挖掘 |
| Developers / Publishers | String | 开发商/发行商 | A4: 厂商生态 |
| Windows / Mac / Linux | Bool | 平台支持 | A4: 跨平台分析 |
| Average playtime | Long | 平均游玩时长 | A1: 生命周期 |
| Supported languages | String | 支持语言(JSON数组) | 辅助 |
| Recommendations | Int | 推荐数 | 热度指标 |

## 分析功能

### 离线批处理（4个分析模块）

| # | 分析名称 | 分析类型 | 核心字段 |
|---|----------|----------|----------|
| A1 | 市场趋势分析 | 时序聚合 + YoY增长 | Release date, Owners, Price, Playtime |
| A2 | 定价与评价关联 | 统计相关性分析 | Price, Positive, Negative, Metacritic |
| A3 | 类型标签关联挖掘 | FP-Growth + 共现分析 | Genres, Tags, Categories |
| A4 | 开发商生态分析 | 市场集中度 HHI | Developers, Publishers, Owners, Platforms |

### 实时流处理（加分项）

- 实时游戏数据流仪表盘
- 滑动窗口统计
- 前端每5秒自动刷新

## 快速开始

```bash
# 编译
sbt compile

# 打包
sbt assembly

# 本地测试（使用样例数据）
spark-submit --class com.spark.project.Main \
             --master local[*] \
             target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar \
             --sample

# 集群提交（使用完整数据）
spark-submit --class com.spark.project.Main \
             --master yarn \
             --deploy-mode cluster \
             --executor-memory 4G \
             --num-executors 4 \
             target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar
```
