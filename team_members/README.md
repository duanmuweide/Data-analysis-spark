# 2026年第6学期 Spark 项目 — 小组提交

> **班级/小组**：CLS01_Group01  
> **提交日期**：2026年7月  
> **技术栈**：Spark 2.4.6 + Scala 2.11 + Kafka 2.5 + Hadoop 3.3.6 + MySQL 8.0 + Tomcat 9

---

## 小组成员及分工

| 角色 | 姓名 | GitHub | 负责模块 | 项目目录 |
|------|------|--------|----------|----------|
| 组长 | **李宇希** | wade | Steam游戏大数据分析 | `wade_李宇希_Steam游戏分析/` |
| 成员 | **陈政铭** | czm | 电商实时分析 | `czm_陈政铭_电商实时分析/` |
| 成员 | **陈家政** | cjz | 房产数据分析 | `cjz_陈家政_房产数据分析/` |

---

## 项目概述

本项目由三人小组合作完成，每人选择一个独立的数据分析主题，使用 Apache Spark 进行大数据离线分析和实时流处理，并通过 Web 仪表盘展示分析结果。

### 李宇希 — Steam 游戏大数据分析
- **数据集**：games.csv（373MB，115,290 款游戏，39 个属性维度）
- **离线分析**：市场趋势、定价评价关联、标签关联挖掘(FP-Growth)、开发商生态分析(HHI)
- **实时分析**：Kafka → Spark Structured Streaming → MySQL → Web 仪表盘
- **技术亮点**：FP-Growth 关联规则挖掘、HHI 市场集中度指数、纯 Spark SQL 清洗（无 UDF）

### 陈政铭 — 电商实时分析
- **数据集**：电商订单数据（orders, order_items, products, users, order_events）
- **离线分析**：品类销售排名、时间趋势、用户价值分层、品类关联规则
- **实时分析**：Kafka → Spark Streaming → MySQL → Flask Web 仪表盘
- **技术亮点**：Python Flask 可视化、ECharts 图表、实时订单流处理

### 陈家政 — 房产数据分析
- **数据集**：房产信息数据（wholedata.csv，含房屋属性、价格、区域等）
- **离线分析**：区域分析、社区分析、行政区分析、年份趋势分析
- **实时分析**：Kafka → Spark Streaming → MySQL → JSP 仪表盘
- **技术亮点**：多格式输入支持（CSV/JSON/Avro/Parquet/ORC）、Hive 集成

---

## 目录结构

```
CLS01_Group01_李宇希_陈政铭_陈家政/
│
├── README.md                              ← 本文件
│
├── wade_李宇希_Steam游戏分析/
│   ├── src/main/scala/com/spark/project/
│   │   ├── Main.scala                     ← 主入口
│   │   ├── common/                        ← 通用工具（Config, Utils, SparkSessionBuilder）
│   │   ├── analysis/member1/              ← 4个离线分析
│   │   │   ├── Analysis1.scala            （市场趋势分析）
│   │   │   ├── Analysis2.scala            （定价与评价关联）
│   │   │   ├── Analysis3.scala            （类型标签关联挖掘）
│   │   │   └── Analysis4.scala            （开发商发行商生态）
│   │   ├── realtime/member1/              ← 实时流处理
│   │   │   └── RealtimeStats.scala
│   │   └── web/                           ← Web 服务
│   │       ├── WebServer.scala
│   │       └── ApiRoutes.scala
│   ├── web/                               ← Java Web 仪表盘
│   │   └── src/main/java/com/spark/web/
│   │       ├── dao/MysqlDao.java
│   │       └── servlet/*.java
│   ├── scripts/                           ← 数据生成 + Kafka 模拟脚本
│   ├── data/member1/                      ← 数据说明
│   ├── 项目提交文档/                       ← 8份提交文档（docx + xlsx）
│   ├── 项目启动指南.md
│   └── README.md
│
├── czm_陈政铭_电商实时分析/
│   ├── chzm/
│   │   ├── ecommerce_analysis/            ← Spark 分析代码（Maven）
│   │   ├── spark_ecommerce_web/           ← Flask Web 仪表盘
│   │   ├── scripts/                       ← 数据生成 + Kafka 脚本
│   │   ├── 项目提交文档/                   ← 8份提交文档
│   │   ├── 项目过程截图/                   ← 运行截图
│   │   └── 各种命令参考.txt
│   ├── data/member1/
│   ├── scripts/
│   └── README.md
│
└── cjz_陈家政_房产数据分析/
    ├── SparkProject/
    │   ├── src/main/scala/com/qdu/
    │   │   ├── jdbc/                       ← 4个离线分析
    │   │   │   ├── AreaAnalysis.scala
    │   │   │   ├── CommunityAnalysis.scala
    │   │   │   ├── DistrictAnalysis.scala
    │   │   │   └── YearAnalysis.scala
    │   │   └── kafka/                      ← Kafka 实时处理
    │   │       ├── Producer.java
    │   │       ├── Consumer.java
    │   │       └── UpdateCertain.java
    │   ├── src/main/resources/             ← 配置文件 + 数据集
    │   └── src/main/webapp/                ← Web 仪表盘
    ├── data/member1/
    ├── scripts/
    └── README.md
```

---

## 快速开始

### 李宇希 — Steam 游戏分析
```bash
# 本地模式开发测试
spark-submit --class com.spark.project.Main --master local[*] \
    target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar --sample

# YARN 集群模式（完整数据）
spark-submit --class com.spark.project.Main --master yarn --deploy-mode cluster \
    --executor-memory 4G --num-executors 3 \
    target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar

# 启动实时流
spark-submit ... --sample --streaming
```
Web 仪表盘：http://localhost:8080

### 陈政铭 — 电商实时分析
详见 `chzm/ecommerce_analysis/README_RUN.md`

### 陈家政 — 房产数据分析
详见项目内 `spark-analysis.sh`

---

## 提交文档清单

每人提交 9 份文档（详见各自 `项目提交文档/` 目录）：

| # | 文档 | 格式 |
|---|------|------|
| 00 | 提交文档目录与去重说明 | .docx |
| 01 | 软件可行性分析报告 | .docx |
| 02 | 项目整体计划与里程碑 | .docx |
| 03 | 人员分工与每日详细计划 | .xlsx |
| 04 | 需求分析报告 | .docx |
| 05 | 数据字典 | .xlsx |
| 06 | 单元测试报告 | .docx |
| 07 | 单元测试用例清单 | .xlsx |
| 08 | 系统分析报告 | .docx |

---

## 技术架构

```
┌─────────────────────────────────────────────────────────┐
│                    展示层 (Browser)                       │
│              ECharts + AJAX 轮询 (5s)                    │
├─────────────────────────────────────────────────────────┤
│            服务层 (Tomcat 9 / Flask)                      │
│          RESTful API + JSP/HTML 页面渲染                  │
├─────────────────────────────────────────────────────────┤
│          存储层 (MySQL 8.0 / HDFS)                       │
│       分析结果表 + 实时统计表                              │
├──────────────────────────┬──────────────────────────────┤
│   批处理层 (Spark SQL)    │  速度层 (Spark Streaming)      │
│   4个离线分析任务         │  Kafka → Structured Streaming  │
├──────────────────────────┴──────────────────────────────┤
│              数据采集层 (CSV / Kafka Producer)            │
└─────────────────────────────────────────────────────────┘
```

---

## 环境信息

| 配置项 | 值 |
|--------|-----|
| VM 主机名 | master-pc |
| JDK | 1.8.0_442 (OpenJDK) |
| Spark | 2.4.6 (/usr/local/spark-2.4.6) |
| Kafka | 2.5.0 (~/kafka_2.12-2.5.0) |
| Hadoop | 3.3.6 |
| MySQL | 8.0 (Windows 宿主机 192.168.211.1) |
| Tomcat | 9.0 |
| 构建工具 | Maven 3.x |

---

## 致谢

感谢老师本学期的指导和帮助！
