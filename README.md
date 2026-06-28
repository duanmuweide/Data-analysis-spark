# 2026年第6学期 Spark 项目 — 小组合作

> 三人小组：wade · czm · cjz

## 项目总览

```
├── 📊 Steam 游戏大数据分析 (wade)
│   ├── src/main/scala/     Spark 离线分析 + 实时流 (Maven)
│   ├── web/                Java Web 仪表盘 (Tomcat 9)
│   ├── scripts/            Kafka 数据模拟器
│   └── 项目启动指南.md      详细启动步骤
│
├── 📊 电商实时分析 (czm)
│   ├── chzm/ecommerce_analysis/      Spark 分析代码
│   ├── chzm/spark_ecommerce_web/     Java Web 仪表盘
│   └── chzm/离线分析执行命令.txt
│
└── 📊 Spark 数据分析 (cjz)
    ├── SparkProject/src/             Scala/Java 源码 (Maven)
    └── SparkProject/pom.xml
```

## 技术栈

| 组件 | 版本 |
|------|------|
| Spark | 2.4.6 |
| Scala | 2.11.12 |
| Kafka | 2.5.0 |
| Hadoop | 3.3.6 |
| JDK | 1.8 |
| MySQL | 8.0 |
| Tomcat | 9.0 |
| 构建 | Maven |

## VM 环境

| 配置 | 值 |
|------|-----|
| 主机名 | master-pc |
| JDK 8 | `/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.442.b06-2.el8.x86_64/jre` |
| Kafka | `~/kafka_2.12-2.5.0` |
| Spark | `/usr/local/spark-2.4.6` |
| Hadoop | `~/cmd-hadoop-launch.sh` |

## MySQL 数据库

```sql
CREATE DATABASE IF NOT EXISTS spark_steam_games;
```

四张离线表 + 一张实时表（Spark 写入，Web 读取）

## 快速开始

各项目的具体启动步骤见各自目录下的说明文档。wade 项目详见 `项目启动指南.md`。
