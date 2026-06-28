package com.qdu.jdbc

import org.apache.spark.sql.SaveMode

import java.util.Properties

object CommunityAnalysis {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("CommunityAnalysis")
      .getOrCreate()
    import spark.implicits._

    // ==================== JDBC连接属性 ====================
    val connProps = new Properties()
    connProps.put("user", "root")
    connProps.put("password", "root")
    connProps.put("driver", "com.mysql.jdbc.Driver")
    val jdbcUrl = "jdbc:mysql://192.168.211.1:3306/cjz_spark"

    // ==================== 第1步：从MySQL读取清洗后的房屋数据 ====================
    val cleanDF = spark.read
      .jdbc(jdbcUrl, "house_info_clean_checkid", connProps)

    println("========== house_info_clean_checkid 表 Schema ==========")
    cleanDF.printSchema()
    println("========== house_info_clean_checkid 数据预览（前10行） ==========")
    cleanDF.show(10, truncate = false)
    println(s"========== 总记录数: ${cleanDF.count()} ==========")

    // 注册临时视图
    cleanDF.createOrReplaceTempView("house_info_clean_checkid")

    // ==================== 第2步：按小区维度分析房价数据 ====================
    // 分析维度说明：
    //   - 按 district（区县） + community（小区）分组
    //   - 分析指标：房屋数量、平均单价
    //   - 建造年份：同一小区取最小年份-最大年份区间，若相同则取单一年份
    //   - 按 checkid（数据批次ID）分组，保留批次信息

    val analysisSQL =
      """
        |SELECT
        |    district,
        |    community,
        |    COUNT(*) AS house_count,
        |    CAST(AVG(price_per_sqm) AS INT) AS avg_price_per_sqm,
        |    CASE
        |        WHEN MIN(build_year) = MAX(build_year) THEN CAST(MIN(build_year) AS STRING)
        |        ELSE CONCAT(CAST(MIN(build_year) AS STRING), '-', CAST(MAX(build_year) AS STRING))
        |    END AS build_year,
        |    checkid
        |FROM house_info_clean_checkid
        |WHERE district IS NOT NULL
        |  AND district != ''
        |  AND community IS NOT NULL
        |  AND community != ''
        |  AND price_per_sqm IS NOT NULL
        |  AND price_per_sqm > 0
        |GROUP BY district, community, checkid
        |ORDER BY district, community, checkid
      """.stripMargin

    println("========== 执行小区维度分析SQL ==========")
    println(analysisSQL)

    val analysisDF = spark.sql(analysisSQL)

    println("========== 分析结果 Schema ==========")
    analysisDF.printSchema()
    println("========== 分析结果预览（前50行） ==========")
    analysisDF.show(50, truncate = false)

    // ==================== 第3步：将分析结果写入 community_price_analysis 表 ====================
    analysisDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "community_price_analysis", connProps)
    println("========== 分析结果已写入 community_price_analysis 表 ==========")

    spark.stop()
  }
}
