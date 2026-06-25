CREATE database cjz_spark;
use cjz_spark;
--建立初始表
CREATE TABLE house_info_checkid (
                                    district VARCHAR(50) COMMENT '市区',
                                    community VARCHAR(100) COMMENT '小区',
                                    layout VARCHAR(50) COMMENT '户型',
                                    orientation VARCHAR(20) COMMENT '朝向',
                                    floor_num INT COMMENT '楼层',
                                    decoration VARCHAR(50) COMMENT '装修情况',
                                    elevator VARCHAR(20) COMMENT '电梯',
                                    area INT COMMENT '面积(㎡)',
                                    price INT COMMENT '价格(万元)',
                                    build_year VARCHAR(10) COMMENT '年份'
) COMMENT='初始房屋信息表';
--插入数据
LOAD DATA LOCAL INFILE '/home/master/sparkproject/wholedata.csv'
INTO TABLE house_info_checkid
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES; -- 如果CSV有表头，请加上这行
--建立清理后的表
CREATE TABLE IF NOT EXISTS house_info_clean_checkid (
                                                        rowkey VARCHAR(255) COMMENT '唯一标识:市区_小区_序号',
    district VARCHAR(50) COMMENT '市区',
    community VARCHAR(100) COMMENT '小区',
    layout VARCHAR(50) COMMENT '户型',
    orientation VARCHAR(20) COMMENT '朝向',
    floor_num INT COMMENT '楼层',
    decoration VARCHAR(50) COMMENT '装修情况',
    elevator_int INT COMMENT '电梯:1有/0无',
    area INT COMMENT '面积(㎡)',
    price INT COMMENT '价格(万元)',
    price_per_sqm INT COMMENT '单价(元/㎡)',
    build_year VARCHAR(10) COMMENT '建造年份',
    house_age INT COMMENT '房龄(年)',
    checkid INT COMMENT '模拟多次查询的批次'
    ) COMMENT='清洗后的房屋信息表';
--插入清理后的数据
INSERT INTO house_info_clean_checkid
SELECT
    -- 生成rowkey（去掉特殊字符）
    CONCAT(
            district, '_',
            REGEXP_REPLACE(community, '[^\\w\\u4e00-\\u9fff]', '_'),
            '_',
            CAST(ROW_NUMBER() OVER (ORDER BY district, community) AS CHAR)
    ) AS rowkey,

    -- 原始字段
    district,
    community,
    layout,
    orientation,
    floor_num,
    decoration,

    -- 电梯字段转换
    CASE
        WHEN elevator = '有电梯' THEN 1
        WHEN elevator = '无电梯' THEN 0
        ELSE 0
        END AS elevator_int,

    area,
    price,

    -- 单价取整（万元转元，再除以面积）
    CAST(ROUND(price * 10000.0 / area) AS SIGNED) AS price_per_sqm,

    build_year,

    -- 计算房龄（假设当前年份为2025）
    (2025 - CAST(build_year AS SIGNED)) AS house_age,

    1 AS checkid

FROM house_info_checkid
WHERE district IS NOT NULL
  AND community IS NOT NULL
  AND area > 0
  AND price > 0
  AND build_year REGEXP '^[0-9]{4}$';

CREATE TABLE IF NOT EXISTS area_price_analysis(
    area_range VARCHAR(100) COMMENT '面积范围',
    house_count INT COMMENT '房屋数量',
    avg_price_per_sqm INT COMMENT '平均单价(元/㎡)',
    min_price INT COMMENT '最低单价(元/㎡)',
    max_price INT COMMENT '最高单价(元/㎡)',
    median_price INT COMMENT '中位数单价(元/㎡)',
    price_variance INT COMMENT '价格方差',
    price_stddev INT COMMENT '价格标准差',
    avg_house_age INT COMMENT '平均房龄(年)',
    load_date VARCHAR(20) COMMENT '加载日期',
    price_level VARCHAR(50) COMMENT '价格等级',
    area_ratio DECIMAL(5,2) COMMENT '面积占比',
    checkid INT COMMENT '数据批次ID',
    pt_date VARCHAR(20) COMMENT '分区日期',

    INDEX idx_area_range (area_range),
    INDEX idx_pt_date (pt_date),
    INDEX idx_checkid (checkid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面积区间房价分析表（含批次）';

CREATE TABLE IF NOT EXISTS district_house_price_analysis(
    district VARCHAR(100) COMMENT '市区名称',
    avg_price_per_sqm INT COMMENT '平均房价(元/平方米)',
    house_count INT COMMENT '房屋数量',
    min_price INT COMMENT '最低单价',
    max_price INT COMMENT '最高单价',
    median_price INT COMMENT '中位数单价',
    price_variance INT COMMENT '价格方差',
    std_price INT COMMENT '价格标准差',
    avg_house_age INT COMMENT '平均房龄(年)',
    avg_area INT COMMENT '平均面积(㎡)',
    checkid INT COMMENT '数据批次ID',
    load_date VARCHAR(20) COMMENT '数据加载日期',

    PRIMARY KEY (district, checkid),  -- 可选：如果每个区每天一条记录
    INDEX idx_district (district),
    INDEX idx_load_date (load_date),
    INDEX idx_checkid (checkid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='北京市区房价统计分析表（带批次）';

CREATE TABLE IF NOT EXISTS house_year_analysis(
    year_range VARCHAR(255) COMMENT '房屋年份区间',
    house_count INT COMMENT '房子数量',
    elevator_count INT COMMENT '电梯数量',
    small_layout_count INT COMMENT '小户型数量(1-2室)',
    medium_layout_count INT COMMENT '中户型数量(3-4室)',
    large_layout_count INT COMMENT '大户型数量(5室以上及其他)',
    premium_decoration_count INT COMMENT '精装数量',
    simple_decoration_count INT COMMENT '简装数量',
    rough_decoration_count INT COMMENT '毛坯数量',
    analysis_time DATETIME COMMENT '分析时间',
    checkid INT COMMENT '批次ID',
    pt_date VARCHAR(20) COMMENT '分区日期',

    INDEX idx_pt_date (pt_date),
    INDEX idx_year_range (year_range)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房屋建造年份与户型、电梯、装修情况关联分析主表';

CREATE TABLE IF NOT EXISTS community_price_analysis (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    checkid INT NOT NULL COMMENT '分析批次ID',
    district VARCHAR(255) NOT NULL COMMENT '区县',
    community VARCHAR(255) NOT NULL COMMENT '小区名称',
    house_count INT NOT NULL COMMENT '房屋数量',
    avg_price_per_sqm INT NOT NULL COMMENT '平均单价（元/平方米）',
    build_year VARCHAR(20) COMMENT '建造年份（原始字符串）',


    -- 可选：记录同步时间
    sync_time DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- 联合唯一索引：确保同一批次下同一小区只有一条记录（幂等）
    UNIQUE KEY uk_checkid_district_community (checkid, district, community),

    -- 普通索引加速查询
    INDEX idx_district (district),
    INDEX idx_community (community),
    INDEX idx_checkid (checkid),
    INDEX idx_avg_price (avg_price_per_sqm)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小区房价分析结果表';


CREATE TABLE IF NOT EXISTS certain_analysis (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    district VARCHAR(100) NOT NULL DEFAULT '海淀' COMMENT '市区名称',
    area VARCHAR(20) NOT NULL DEFAULT '90-144㎡' COMMENT '面积区间',
    community VARCHAR(255) NOT NULL COMMENT '小区名称',
    number INT NOT NULL COMMENT '满足条件的房屋数量',
    averageprice INT NOT NULL COMMENT '房屋平均单价(元/㎡)',
    sync_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '同步时间',
    UNIQUE KEY uk_community (community),
    INDEX idx_district (district),
    INDEX idx_area (area)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='海淀区90-144㎡小区房价实时分析表';