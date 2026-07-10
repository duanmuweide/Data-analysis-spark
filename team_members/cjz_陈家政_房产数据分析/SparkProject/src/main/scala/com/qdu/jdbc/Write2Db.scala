package com.qdu.jdbc

import org.apache.spark.sql.SaveMode

import java.util.Properties

object Write2Db {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("Write2DB")
      .getOrCreate()
    import spark.implicits._

    // ==================== JDBC连接属性（保留原有配置） ====================
    val connProps = new Properties()
    connProps.put("user", "root")
    connProps.put("password", "root")
    connProps.put("driver", "com.mysql.jdbc.Driver")
    val jdbcUrl = "jdbc:mysql://192.168.211.1:3306/cjz_spark"

    // ==================== 第1步：LOAD DATA LOCAL INFILE 等价操作 ====================
    // 读取CSV文件 /home/master/sparkproject/wholedata.csv
    // CHARACTER SET utf8mb4 → Spark默认使用UTF-8
    // FIELDS TERMINATED BY ',' → delimiter
    // LINES TERMINATED BY '\n' → Spark默认换行
    // IGNORE 1 LINES → header=true 跳过第一行
    val rawDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .option("inferSchema", "true")
      .csv("/home/master/sparkproject/wholedata.csv")
      // CSV文件列名为中文，需重命名为英文以匹配MySQL表字段和SQL中的列名
      .withColumnRenamed("市区", "district")
      .withColumnRenamed("小区", "community")
      .withColumnRenamed("户型", "layout")
      .withColumnRenamed("朝向", "orientation")
      .withColumnRenamed("楼层", "floor_num")
      .withColumnRenamed("装修情况", "decoration")
      .withColumnRenamed("电梯", "elevator")
      .withColumnRenamed("面积(㎡)", "area")
      .withColumnRenamed("价格(万元)", "price")
      .withColumnRenamed("年份", "build_year")

    println("========== 原始数据 Schema ==========")
    rawDF.printSchema()
    println("========== 原始数据预览（前10行） ==========")
    rawDF.show(10, truncate = false)

    // 将原始数据写入 house_info_checkid 表（等价于 LOAD DATA INFILE INTO house_info_checkid）
    rawDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "house_info_checkid", connProps)
    println("========== 原始数据已写入 house_info_checkid 表 ==========")

    // ==================== 第2步：INSERT INTO ... SELECT 等价操作 ====================
    // 注册临时视图，用于执行Spark SQL
    rawDF.createOrReplaceTempView("house_info_checkid")

    // 执行转换SQL（等价于 INSERT INTO house_info_clean_checkid SELECT ...）
    // 使用子查询先将 ROW_NUMBER 计算出来，再在外层拼接 rowkey
    val transformSQL =
      """
        |SELECT
        |    CONCAT(
        |        district, '_',
        |        regexp_replace(community, '[^\w一-鿿]', '_'),
        |        '_',
        |        CAST(rn AS STRING)
        |    ) AS rowkey,
        |    district,
        |    community,
        |    layout,
        |    orientation,
        |    floor_num,
        |    decoration,
        |    CASE
        |        WHEN elevator = '有电梯' THEN 1
        |        WHEN elevator = '无电梯' THEN 0
        |        ELSE 0
        |    END AS elevator_int,
        |    area,
        |    price,
        |    CAST(ROUND(price * 10000.0 / area) AS INT) AS price_per_sqm,
        |    build_year,
        |    (2025 - CAST(build_year AS INT)) AS house_age,
        |    1 AS checkid
        |FROM (
        |    SELECT
        |        *,
        |        ROW_NUMBER() OVER (ORDER BY district, community) AS rn
        |    FROM house_info_checkid
        |    WHERE district IS NOT NULL
        |      AND community IS NOT NULL
        |      AND area > 0
        |      AND price > 0
        |      AND build_year RLIKE '^[0-9]{4}$'
        |) t
      """.stripMargin

    println("========== 执行转换SQL ==========")
    println(transformSQL)

    val cleanDF = spark.sql(transformSQL)

    println("========== 清洗后数据 Schema ==========")
    cleanDF.printSchema()
    println("========== 清洗后数据预览（前10行） ==========")
    cleanDF.show(10, truncate = false)

    // 将清洗后的数据写入 house_info_clean_checkid 表
    cleanDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "house_info_clean_checkid", connProps)
    println("========== 清洗数据已写入 house_info_clean_checkid 表 ==========")

    spark.stop()
  }
}
