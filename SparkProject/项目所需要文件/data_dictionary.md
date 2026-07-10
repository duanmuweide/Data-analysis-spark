# 数据字典 — SparkSQL 房源信息分析系统

> **数据库名称**: `cjz_spark`
> **生成日期**: 2026-06-24
> **字符集**: utf8mb4
> **存储引擎**: InnoDB

---

## 目录

1. [系统概述](#系统概述)
2. [数据分层架构](#数据分层架构)
3. [数据流转图](#数据流转图)
4. [表结构详情](#表结构详情)
   - [ODS 层：house_info_checkid — 原始房源信息表](#1-house_info_checkid)
   - [DWD 层：house_info_clean_checkid — 清洗后房源信息表](#2-house_info_clean_checkid)
   - [ADS 层：area_price_analysis — 面积区间房价分析结果表](#3-area_price_analysis)
   - [ADS 层：district_house_price_analysis — 市区房价统计分析结果表](#4-district_house_price_analysis)
   - [ADS 层：house_year_analysis — 建造年份区间分析结果表](#5-house_year_analysis)
   - [ADS 层：community_price_analysis — 小区房价分析结果表](#6-community_price_analysis)
   - [ADS 层：certain_analysis — 特定条件小区实时数据表](#7-certain_analysis)
   - [辅助表：kafka_producer_progress — Kafka 生产者进度表](#8-kafka_producer_progress)

---

## 系统概述

本系统是基于 **SparkSQL + Kafka + MySQL** 的房源信息分析平台，主要功能包括：

- 从 CSV 文件加载原始房源数据
- 对原始数据进行清洗与转换（数据脱敏、字段标准化、派生字段计算）
- 按多维度进行聚合分析（面积区间、市区、建造年份、小区）
- 通过 Kafka 实时流处理特定条件（海淀区 90-144㎡）的房源数据
- 将分析结果写入 MySQL 结果表，供前端或报表使用

**技术栈**: SparkSQL / Scala 2.12 / Java (Kafka Client) / Kafka / MySQL (InnoDB) / JDBC

**程序清单**:

| 程序文件 | 语言 | 功能说明 |
|---|---|---|
| `Write2Db.scala` | Scala | 读取 CSV → 写入 ODS 表 → 清洗转换 → 写入 DWD 表 |
| `AreaAnalysis.scala` | Scala | 从 DWD 读取 → 面积区间维度分析 → 写入 ADS 表 |
| `DistrictAnalysis.scala` | Scala | 从 DWD 读取 → 市区维度分析 → 写入 ADS 表 |
| `YearAnalysis.scala` | Scala | 从 DWD 读取 → 建造年份维度分析 → 写入 ADS 表 |
| `CommunityAnalysis.scala` | Scala | 从 DWD 读取 → 小区维度分析 → 写入 ADS 表 |
| `Producer.java` | Java | 从 DWD 轮询海淀区 90-144㎡ 数据 → 发送到 Kafka Topic |
| `Consumer.java` | Java | 从 Kafka 消费消息 → 内存聚合 → 写入 `certain_analysis` 表 |

---

## 数据分层架构

```
┌─────────────────────────────────────────────────┐
│  ODS 层（操作数据层）                              │
│  house_info_checkid          ← CSV 原始数据导入    │
└────────────────────┬────────────────────────────┘
                     │ Write2Db.scala（清洗 & 转换）
                     ▼
┌─────────────────────────────────────────────────┐
│  DWD 层（数据仓库明细层）                          │
│  house_info_clean_checkid    ← 清洗后的标准化数据   │
└──┬───────┬──────────┬──────────┬──────────┬─────┘
   │       │          │          │          │
   │ Area  │ District │ Year     │ Community│  Producer.java
   │Analy‐ │ Analysis │ Analysis │ Analysis │  (轮询checkid)
   │ sis   │ .scala   │ .scala   │ .scala   │
   │.scala │          │          │          │
   ▼       ▼          ▼          ▼          ▼
┌──────────────────────────────┐  ┌──────────────────────┐
│  ADS 层（批处理结果）          │  │  Kafka 消息队列        │
│  area_price_analysis         │  │  Topic: "kafka"       │
│  district_house_price_……     │  │                       │
│  house_year_analysis         │  │  Consumer.java        │
│  community_price_analysis    │  │  (实时聚合消费)        │
└──────────────────────────────┘  └──────────┬───────────┘
                                             │
                                             ▼
                                  ┌──────────────────────┐
                                  │  ADS 层（流处理结果）   │
                                  │  certain_analysis     │
                                  └──────────────────────┘
```

---

## 数据流转图

### 批处理链路（SparkSQL）

```
wholedata.csv
     │
     │ LOAD DATA / Spark read
     ▼
house_info_checkid (ODS)
     │
     │ INSERT INTO ... SELECT (SparkSQL 清洗转换)
     │   - 生成 rowkey
     │   - elevator → elevator_int
     │   - 计算 price_per_sqm、house_age
     │   - 数据过滤（NOT NULL, >0, 年份正则）
     ▼
house_info_clean_checkid (DWD)
     │
     ├── AreaAnalysis ──────► area_price_analysis
     ├── DistrictAnalysis ──► district_house_price_analysis
     ├── YearAnalysis ──────► house_year_analysis
     └── CommunityAnalysis ─► community_price_analysis
```

### 流处理链路（Kafka）

```
house_info_clean_checkid (DWD)
     │
     │ Producer.java 每秒轮询
     │   - 筛选: district='海淀', area 90-144㎡
     │   - checkid 自增批次管理
     │   - 断点续跑 (kafka_producer_progress)
     ▼
Kafka Topic "kafka"
     │ (JSON 消息: {community, price_per_sqm})
     │ (批次结束标记: {__END_OF_BATCH__, checkid})
     ▼
Consumer.java 实时消费
     │   - 内存聚合 (ConcurrentHashMap)
     │   - 收到批次结束标记 → 触发写入
     ▼
certain_analysis (ADS)
     │   - INSERT ... ON DUPLICATE KEY UPDATE
     │   - 加权平均合并新旧数据
```

---

## 表结构详情

---

### 1. house_info_checkid

**表名**: `house_info_checkid`
**中文名**: 原始房源信息表
**数据层**: ODS（操作数据层）
**表说明**: 存储从 CSV 文件直接导入的原始房源数据，保留数据的原始面貌，不做任何清洗转换。
**数据来源**: `/home/master/sparkproject/wholedata.csv`（CSV 文件，UTF-8 编码，逗号分隔，含表头行）
**写入程序**: `Write2Db.scala`
**主键**: 无

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `district` | VARCHAR(50) | YES | NULL | 市区 | CSV 列"市区"，如"市南"、"市北"、"崂山"等 |
| 2 | `community` | VARCHAR(100) | YES | NULL | 小区名称 | CSV 列"小区" |
| 3 | `layout` | VARCHAR(50) | YES | NULL | 户型 | CSV 列"户型"，如"2室1厅"、"3室2厅" |
| 4 | `orientation` | VARCHAR(20) | YES | NULL | 朝向 | CSV 列"朝向"，如"南"、"南北"、"东南" |
| 5 | `floor_num` | INT | YES | NULL | 楼层 | CSV 列"楼层"，整数 |
| 6 | `decoration` | VARCHAR(50) | YES | NULL | 装修情况 | CSV 列"装修情况"，如"精装"、"简装"、"毛坯" |
| 7 | `elevator` | VARCHAR(20) | YES | NULL | 电梯 | CSV 列"电梯"，取值："有电梯" / "无电梯" |
| 8 | `area` | INT | YES | NULL | 面积(㎡) | CSV 列"面积(㎡)"，整数，单位：平方米 |
| 9 | `price` | INT | YES | NULL | 价格(万元) | CSV 列"价格(万元)"，整数，单位：万元 |
| 10 | `build_year` | VARCHAR(10) | YES | NULL | 建造年份 | CSV 列"年份"，如"2008"、"2015" |

---

### 2. house_info_clean_checkid

**表名**: `house_info_clean_checkid`
**中文名**: 清洗后房源信息表
**数据层**: DWD（数据仓库明细层）
**表说明**: 存储经过清洗、转换和过滤后的标准化房源数据。对原始数据做了字段类型转换、派生字段计算、无效数据过滤等处理。本表是批处理分析链路和流处理链路的共同数据源。
**数据来源**: `house_info_checkid` 表，经 SparkSQL 清洗转换
**写入程序**: `Write2Db.scala`
**读取程序**: `AreaAnalysis.scala`、`DistrictAnalysis.scala`、`YearAnalysis.scala`、`CommunityAnalysis.scala`、`Producer.java`
**主键**: 无（rowkey 为业务唯一标识）

#### 清洗规则

| 规则类别 | 规则描述 |
|----------|----------|
| 数据过滤 | `district IS NOT NULL`、`community IS NOT NULL`、`area > 0`、`price > 0`、`build_year` 匹配 `^[0-9]{4}$` |
| 字段转换 | `elevator` → `elevator_int`（有电梯=1，无电梯=0） |
| 派生计算 | `price_per_sqm = ROUND(price × 10000 / area)` |
| 派生计算 | `house_age = 2025 - CAST(build_year AS INT)` |
| 标识生成 | `rowkey = CONCAT(district, '_', REGEXP_REPLACE(community, '[^\w一-鿿]', '_'), '_', ROW_NUMBER())` |
| 批次标识 | `checkid` 固定为 1 |

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `rowkey` | VARCHAR(255) | YES | NULL | 唯一标识 | 格式：`{district}_{community_clean}_{序号}`，其中 community_clean 为去除特殊字符后的小区名 |
| 2 | `district` | VARCHAR(50) | YES | NULL | 市区 | 继承自 `house_info_checkid.district` |
| 3 | `community` | VARCHAR(100) | YES | NULL | 小区名称 | 继承自 `house_info_checkid.community` |
| 4 | `layout` | VARCHAR(50) | YES | NULL | 户型 | 继承自 `house_info_checkid.layout` |
| 5 | `orientation` | VARCHAR(20) | YES | NULL | 朝向 | 继承自 `house_info_checkid.orientation` |
| 6 | `floor_num` | INT | YES | NULL | 楼层 | 继承自 `house_info_checkid.floor_num` |
| 7 | `decoration` | VARCHAR(50) | YES | NULL | 装修情况 | 继承自 `house_info_checkid.decoration` |
| 8 | `elevator_int` | INT | YES | NULL | 电梯(数值) | 1=有电梯，0=无电梯 |
| 9 | `area` | INT | YES | NULL | 面积(㎡) | 继承自 `house_info_checkid.area` |
| 10 | `price` | INT | YES | NULL | 价格(万元) | 继承自 `house_info_checkid.price` |
| 11 | `price_per_sqm` | INT | YES | NULL | 单价(元/㎡) | 计算：`price × 10000 / area`（四舍五入取整） |
| 12 | `build_year` | VARCHAR(10) | YES | NULL | 建造年份 | 继承自 `house_info_checkid.build_year` |
| 13 | `house_age` | INT | YES | NULL | 房龄(年) | 计算：`2025 - build_year` |
| 14 | `checkid` | INT | YES | NULL | 批次标识 | 固定值 1，用于模拟多批次数据查询 |

---

### 3. area_price_analysis

**表名**: `area_price_analysis`
**中文名**: 面积区间房价分析结果表
**数据层**: ADS（应用数据服务层）
**表说明**: 按面积区间维度对房源数据进行聚合分析，统计各面积区间的房价指标，用于分析不同面积段房源的定价特征。
**数据来源**: `house_info_clean_checkid` 表，经 SparkSQL 聚合计算
**写入程序**: `AreaAnalysis.scala`
**主键**: 无
**索引**: `idx_area_range`(area_range)、`idx_pt_date`(pt_date)、`idx_checkid`(checkid)

#### 分析维度与规则

| 维度 | 计算规则 |
|------|----------|
| 面积区间 | 50-90㎡ / 90-144㎡ / 144-236㎡ / 236㎡以上（area < 50 归为"未知"并过滤） |
| 价格等级 | 低价位(<8000) / 中等价位(8000-15000) / 中高价位(15000-25000) / 高价位(>25000)（单位：元/㎡） |
| 面积占比 | 该区间房屋数 / 总房屋数 × 100%（保留2位小数） |
| 中位数 | `percentile_approx(price_per_sqm, 0.5)` |

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `area_range` | VARCHAR(100) | YES | NULL | 面积范围 | 枚举：`50-90㎡` / `90-144㎡` / `144-236㎡` / `236㎡以上` |
| 2 | `house_count` | INT | YES | NULL | 房源数量 | 该区间内的总房源数 COUNT(*) |
| 3 | `avg_price_per_sqm` | INT | YES | NULL | 平均单价(元/㎡) | AVG(price_per_sqm) 取整 |
| 4 | `min_price` | INT | YES | NULL | 最低单价(元/㎡) | MIN(price_per_sqm) |
| 5 | `max_price` | INT | YES | NULL | 最高单价(元/㎡) | MAX(price_per_sqm) |
| 6 | `median_price` | INT | YES | NULL | 中位数单价(元/㎡) | percentile_approx(price_per_sqm, 0.5) |
| 7 | `price_variance` | INT | YES | NULL | 价格方差 | VARIANCE(price_per_sqm) |
| 8 | `price_stddev` | INT | YES | NULL | 价格标准差 | STDDEV(price_per_sqm) |
| 9 | `avg_house_age` | INT | YES | NULL | 平均房龄(年) | AVG(house_age) 取整 |
| 10 | `load_date` | VARCHAR(20) | YES | NULL | 加载日期 | DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') |
| 11 | `price_level` | VARCHAR(50) | YES | NULL | 价格等级 | 枚举：`低价位` / `中等价位` / `中高价位` / `高价位` |
| 12 | `area_ratio` | DECIMAL(5,2) | YES | NULL | 面积占比(%) | `house_count / 总房源数 × 100` |
| 13 | `checkid` | INT | YES | NULL | 批次标识 | 继承自 DWD 表，用于数据分批 |
| 14 | `pt_date` | VARCHAR(20) | YES | NULL | 分区日期 | DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd')，用于数据分区 |

---

### 4. district_house_price_analysis

**表名**: `district_house_price_analysis`
**中文名**: 市区房价统计分析结果表
**数据层**: ADS（应用数据服务层）
**表说明**: 按市区维度对房源数据进行聚合分析，对比不同行政区的房价水平、房龄分布、面积特征等指标。
**数据来源**: `house_info_clean_checkid` 表，经 SparkSQL 聚合计算
**写入程序**: `DistrictAnalysis.scala`
**主键**: PRIMARY KEY (district, checkid)
**索引**: `idx_district`(district)、`idx_load_date`(load_date)、`idx_checkid`(checkid)

#### 分析规则

- 分组维度：`district` + `checkid`
- 过滤条件：district 非空非空串、price_per_sqm > 0、area > 0
- 中位数：`percentile_approx(price_per_sqm, 0.5)`

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `district` | VARCHAR(100) | NO | — | 市区名称 | 分组字段，如"市南"、"市北"、"崂山"等 |
| 2 | `avg_price_per_sqm` | INT | YES | NULL | 平均单价(元/㎡) | AVG(price_per_sqm) 取整 |
| 3 | `house_count` | INT | YES | NULL | 房源数量 | COUNT(*) |
| 4 | `min_price` | INT | YES | NULL | 最低单价(元/㎡) | MIN(price_per_sqm) |
| 5 | `max_price` | INT | YES | NULL | 最高单价(元/㎡) | MAX(price_per_sqm) |
| 6 | `median_price` | INT | YES | NULL | 中位数单价(元/㎡) | percentile_approx(price_per_sqm, 0.5) |
| 7 | `price_variance` | INT | YES | NULL | 价格方差 | VARIANCE(price_per_sqm) |
| 8 | `std_price` | INT | YES | NULL | 价格标准差 | STDDEV(price_per_sqm) |
| 9 | `avg_house_age` | INT | YES | NULL | 平均房龄(年) | AVG(house_age) 取整 |
| 10 | `avg_area` | INT | YES | NULL | 平均面积(㎡) | AVG(area) 取整 |
| 11 | `checkid` | INT | NO | — | 批次标识 | 主键一部分，继承自 DWD 表 |
| 12 | `load_date` | VARCHAR(20) | YES | NULL | 数据加载日期 | DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') |

---

### 5. house_year_analysis

**表名**: `house_year_analysis`
**中文名**: 建造年份区间分析结果表
**数据层**: ADS（应用数据服务层）
**表说明**: 按建造年份区间维度对房源数据进行聚合分析，分析不同年代建造的房屋在户型分布、电梯配置、装修档次等方面的特征差异。
**数据来源**: `house_info_clean_checkid` 表，经 SparkSQL 聚合计算
**写入程序**: `YearAnalysis.scala`
**主键**: 无
**索引**: `idx_pt_date`(pt_date)、`idx_year_range`(year_range)

#### 分析维度与规则

| 维度 | 计算规则 |
|------|----------|
| 年份区间 | 1950-1970 / 1970-1990 / 1990-2000 / 2000-2010 / 2010-2020（超出范围归为"其他"并过滤） |
| 小户型 | layout 中室数为 1-2（通过正则 `^([0-9])室` 提取） |
| 中户型 | layout 中室数为 3-4 |
| 大户型 | layout 中室数 ≥ 5 或无法识别（room_count = -1） |
| 精装 | decoration = '精装' |
| 简装 | decoration = '简装' |
| 毛坯 | decoration = '毛坯' |

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `year_range` | VARCHAR(255) | YES | NULL | 建造年份区间 | 枚举：`1950-1970` / `1970-1990` / `1990-2000` / `2000-2010` / `2010-2020` |
| 2 | `house_count` | INT | YES | NULL | 房源数量 | COUNT(*)，该区间内的总房源数 |
| 3 | `elevator_count` | INT | YES | NULL | 电梯数量 | SUM(elevator_int)，即有电梯的房源数 |
| 4 | `small_layout_count` | INT | YES | NULL | 小户型数量 | 室数为 1-2 的房源数 |
| 5 | `medium_layout_count` | INT | YES | NULL | 中户型数量 | 室数为 3-4 的房源数 |
| 6 | `large_layout_count` | INT | YES | NULL | 大户型数量 | 室数 ≥ 5 或无法识别的房源数 |
| 7 | `premium_decoration_count` | INT | YES | NULL | 精装数量 | decoration = '精装' 的房源数 |
| 8 | `simple_decoration_count` | INT | YES | NULL | 简装数量 | decoration = '简装' 的房源数 |
| 9 | `rough_decoration_count` | INT | YES | NULL | 毛坯数量 | decoration = '毛坯' 的房源数 |
| 10 | `analysis_time` | DATETIME | YES | NULL | 分析时间 | NOW()，记录分析执行的具体时间 |
| 11 | `checkid` | INT | YES | NULL | 批次标识 | 继承自 DWD 表 |
| 12 | `pt_date` | VARCHAR(20) | YES | NULL | 分区日期 | DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') |

---

### 6. community_price_analysis

**表名**: `community_price_analysis`
**中文名**: 小区房价分析结果表
**数据层**: ADS（应用数据服务层）
**表说明**: 按小区维度对房源数据进行聚合分析，展示每个小区的房源数量、平均单价、建造年份范围等信息，支撑小区级别的房价对比。
**数据来源**: `house_info_clean_checkid` 表，经 SparkSQL 聚合计算
**写入程序**: `CommunityAnalysis.scala`
**主键**: `id` (BIGINT AUTO_INCREMENT)
**唯一约束**: UNIQUE KEY `uk_checkid_district_community` (checkid, district, community)
**索引**: `idx_district`(district)、`idx_community`(community)、`idx_checkid`(checkid)、`idx_avg_price`(avg_price_per_sqm)

#### 分析规则

- 分组维度：`district` + `community` + `checkid`
- 建造年份：同一小区有多年份时显示范围 `MIN-MAX`，单一年份时只显示该年份
- 过滤条件：district 非空、community 非空、price_per_sqm > 0

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `id` | BIGINT | NO | AUTO_INCREMENT | 自增主键 | 系统自动生成 |
| 2 | `checkid` | INT | NO | — | 批次标识 | 继承自 DWD 表 |
| 3 | `district` | VARCHAR(255) | NO | — | 市区 | 分组字段 |
| 4 | `community` | VARCHAR(255) | NO | — | 小区名称 | 分组字段 |
| 5 | `house_count` | INT | NO | — | 房源数量 | COUNT(*)，该小区的总房源数 |
| 6 | `avg_price_per_sqm` | INT | NO | — | 平均单价(元/㎡) | AVG(price_per_sqm) 取整 |
| 7 | `build_year` | VARCHAR(20) | YES | NULL | 建造年份 | 同一年份显示单值，不同年份显示 `MIN-MAX` 范围 |
| 8 | `sync_time` | DATETIME | YES | CURRENT_TIMESTAMP | 同步时间 | 记录写入时间，默认当前时间 |

---

### 7. certain_analysis

**表名**: `certain_analysis`
**中文名**: 特定条件小区实时数据表
**数据层**: ADS（应用数据服务层）
**表说明**: 存储特定条件（海淀区 + 面积 90-144㎡）下的各小区的房源数量和均价，通过 Kafka 流处理链路实时写入。每条记录对应一个唯一小区，支持增量更新（加权平均合并）。
**数据来源**: `house_info_clean_checkid` 表 → Kafka Topic "kafka" → Consumer 聚合写入
**写入程序**: `Consumer.java`
**数据筛选**: `Producer.java` 从 DWD 表中筛选 `district='海淀' AND area >= 90 AND area < 144`
**主键**: `id` (BIGINT AUTO_INCREMENT)
**唯一约束**: UNIQUE KEY `uk_community` (community)
**索引**: `idx_district`(district)、`idx_area`(area)

#### 流处理写入逻辑

```
Producer.java (每秒轮询)
  │
  │ 筛选: district='海淀' AND area ∈ [90, 144)
  │ 序列化: {"community":"xxx","price_per_sqm":nnn}
  │ 批次结束: {"__END_OF_BATCH__":true,"checkid":x}
  ▼
Kafka Topic "kafka"
  ▼
Consumer.java (实时消费)
  │ 内存聚合: ConcurrentHashMap<community, [count, sum_price]>
  │ 收到 __END_OF_BATCH__ → flushToMySQL()
  ▼
certain_analysis
  │ INSERT ... ON DUPLICATE KEY UPDATE
  │ 加权合并公式:
  │   new_avg = (old_avg × old_count + new_avg × new_count)
  │           / (old_count + new_count)
```

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `id` | BIGINT | NO | AUTO_INCREMENT | 自增主键 | 系统自动生成 |
| 2 | `district` | VARCHAR(100) | NO | '未知' | 所属市区 | 固定为 `海淀`（由 Producer 筛选条件决定） |
| 3 | `area` | VARCHAR(20) | NO | '90-144㎡' | 面积区间 | 固定为 `90-144㎡`（由 Producer 筛选条件决定） |
| 4 | `community` | VARCHAR(255) | NO | — | 小区名称 | 唯一键，每个小区一条记录 |
| 5 | `number` | INT | NO | — | 房源数量 | 该小区在筛选范围内的房源总数（支持增量累加） |
| 6 | `averageprice` | INT | NO | — | 平均单价(元/㎡) | 该小区的加权平均单价（支持增量合并） |
| 7 | `sync_time` | DATETIME | YES | CURRENT_TIMESTAMP | 同步时间 | 记录更新时间，ON UPDATE 自动刷新 |

---

### 8. kafka_producer_progress

**表名**: `kafka_producer_progress`
**中文名**: Kafka 生产者进度表
**数据层**: 辅助表（元数据）
**表说明**: 记录 Kafka Producer 上次处理完成的 `checkid`，用于实现断点续跑机制。Producer 启动时读取此表确定从哪个 checkid 开始处理，每完成一个批次后更新。
**数据来源**: `Producer.java` 自动创建和维护
**写入程序**: `Producer.java`
**主键**: `id` (INT, 固定为 1)

#### 断点续跑机制

```
Producer 启动
  │
  ├── 1. getLastProcessedCheckId()
  │      CREATE TABLE IF NOT EXISTS kafka_producer_progress
  │      SELECT COALESCE(MAX(last_checkid), 0) → lastId
  │      currentCheckId = lastId + 1
  │
  ├── 2. 轮询 house_info_clean_checkid WHERE checkid = currentCheckId
  │      ├── 有数据 → 发送 Kafka → saveProcessedCheckId(currentCheckId) → currentCheckId++
  │      └── 无数据 → 等待 1 秒后重试
  │
  └── 3. saveProcessedCheckId()
         INSERT INTO kafka_producer_progress (id, last_checkid)
         VALUES (1, checkId)
         ON DUPLICATE KEY UPDATE last_checkid = checkId
```

#### 字段列表

| 序号 | 字段名 | 数据类型 | 允许空 | 默认值 | 字段说明 | 来源/取值说明 |
|------|--------|----------|--------|--------|----------|--------------|
| 1 | `id` | INT | NO | 1 | 固定主键 | 始终为 1（单行记录表） |
| 2 | `last_checkid` | INT | NO | 0 | 上次完成的批次号 | 每完成一个批次更新，首次运行为 0 |

---

## 附录

### A. Kafka 消息规格

| 属性 | 值 |
|---|---|
| Topic 名称 | `kafka` |
| Kafka 集群 | `hadoop101:9092, niit-slaves1:9092, niit-slaves2:9092` |
| 消息 Key/Value 序列化 | StringSerializer / StringDeserializer |
| Consumer Group | `kafka` |
| Offset 策略 | `earliest`（从最早消息开始消费） |
| Producer ACKS | `all`（所有副本确认） |
| Producer 重试 | 3 次 |
| 轮询间隔 | 1000ms（Producer 每 1 秒检查一次新批次） |
| Consumer Poll 超时 | 100ms |

#### 消息格式

**数据消息**（每条房源记录一条）:
```json
{"community":"小区名称","price_per_sqm":单价整数}
```

**批次结束标记**（一个批次所有数据发送完毕后发送）:
```json
{"__END_OF_BATCH__":true,"checkid":批次号}
```

### B. CSV 源数据列名映射

| CSV 列名（中文） | 数据库字段名 | 目标表 |
|---|---|---|
| 市区 | district | house_info_checkid |
| 小区 | community | house_info_checkid |
| 户型 | layout | house_info_checkid |
| 朝向 | orientation | house_info_checkid |
| 楼层 | floor_num | house_info_checkid |
| 装修情况 | decoration | house_info_checkid |
| 电梯 | elevator | house_info_checkid |
| 面积(㎡) | area | house_info_checkid |
| 价格(万元) | price | house_info_checkid |
| 年份 | build_year | house_info_checkid |

### C. 枚举值速查表

| 字段 | 所属表 | 枚举值 |
|---|---|---|
| elevator | house_info_checkid | `有电梯`、`无电梯` |
| elevator_int | house_info_clean_checkid / house_year_analysis | `1`(有电梯)、`0`(无电梯) |
| decoration | house_info_checkid / house_info_clean_checkid | `精装`、`简装`、`毛坯` |
| area_range | area_price_analysis | `50-90㎡`、`90-144㎡`、`144-236㎡`、`236㎡以上` |
| price_level | area_price_analysis | `低价位`、`中等价位`、`中高价位`、`高价位` |
| year_range | house_year_analysis | `1950-1970`、`1970-1990`、`1990-2000`、`2000-2010`、`2010-2020` |

### D. checkid 说明

`checkid` 字段贯穿 ODS → DWD → ADS 全链路，用于标识数据批次。当前 SparkSQL 批处理实现中固定为 `1`，设计意图是支持多批次数据的增量分析和历史对比。

在 **Kafka 流处理链路**中，`checkid` 被赋予实际意义：`Producer.java` 从 1 开始自增轮询，每次检查 DWD 表中对应 `checkid` 是否有新数据，处理完成后递增。`kafka_producer_progress` 表持久化进度，保证重启后不丢失、不重复。

在 `district_house_price_analysis` 表中，`(district, checkid)` 构成联合主键，可保存同一市区在不同批次下的分析快照。

### E. 中位数计算说明

中位数统一使用 SparkSQL 内置的 `percentile_approx(CAST(price_per_sqm AS DOUBLE), 0.5)` 进行近似计算。这是一个基于 T-Digest 算法的近似百分位数函数，适用于大数据量场景，在精度和性能之间取得平衡。

### F. certain_analysis 加权平均合并公式

Consumer 写入 `certain_analysis` 时使用 `INSERT ... ON DUPLICATE KEY UPDATE`，合并公式为：

```
new_avg = (old_avg × old_count + incoming_avg × incoming_count)
        / (old_count + incoming_count)
```

对应 SQL：
```sql
averageprice = CAST((averageprice * number + VALUES(averageprice) * VALUES(number))
                    / (number + VALUES(number)) AS SIGNED)
number = number + VALUES(number)
```

这保证了多次写入同一小区的数据时，最终的平均单价是全局加权平均，而非简单覆盖。

### G. 连接信息

| 配置项 | 值 |
|---|---|
| JDBC URL | `jdbc:mysql://192.168.211.1:3306/cjz_spark` |
| JDBC Driver | `com.mysql.jdbc.Driver` |
| 数据库 | `cjz_spark` |
| Kafka Bootstrap Servers | `hadoop101:9092, niit-slaves1:9092, niit-slaves2:9092` |
| 字符集 | utf8mb4 |
| 存储引擎 | InnoDB |
