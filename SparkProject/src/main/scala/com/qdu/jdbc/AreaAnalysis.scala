package com.qdu.jdbc

import org.apache.spark.sql.SaveMode

import java.util.Properties

object AreaAnalysis {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("AreaAnalysis")
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

    // ==================== 第2步：面积区间房价分析 ====================
    // 分析维度说明：
    //   - 面积区间划分：50-90㎡ / 90-144㎡ / 144-236㎡ / 236㎡以上
    //   - 价格等级：低价位(<8000) / 中等价位(8000-15000) / 中高价位(15000-25000) / 高价位(>25000)（元/㎡）
    //   - 面积占比：该区间房屋数 / 总房屋数 × 100%
    //   - 中位数：使用 percentile_approx 近似计算

    val analysisSQL =
      """
        |WITH area_classified AS (
        |    SELECT
        |        area,
        |        price_per_sqm,
        |        house_age,
        |        checkid,
        |        CASE
        |            WHEN area >= 50 AND area < 90   THEN '50-90㎡'
        |            WHEN area >= 90 AND area < 144  THEN '90-144㎡'
        |            WHEN area >= 144 AND area < 236 THEN '144-236㎡'
        |            WHEN area >= 236                THEN '236㎡以上'
        |            ELSE '未知'
        |        END AS area_range
        |    FROM house_info_clean_checkid
        |    WHERE area IS NOT NULL
        |      AND area > 0
        |      AND price_per_sqm IS NOT NULL
        |      AND price_per_sqm > 0
        |),
        |stats AS (
        |    SELECT
        |        area_range,
        |        COUNT(*) AS house_count,
        |        CAST(AVG(price_per_sqm) AS INT) AS avg_price_per_sqm,
        |        CAST(MIN(price_per_sqm) AS INT) AS min_price,
        |        CAST(MAX(price_per_sqm) AS INT) AS max_price,
        |        CAST(percentile_approx(CAST(price_per_sqm AS DOUBLE), 0.5) AS INT) AS median_price,
        |        CAST(VARIANCE(price_per_sqm) AS INT) AS price_variance,
        |        CAST(STDDEV(price_per_sqm) AS INT) AS price_stddev,
        |        CAST(AVG(house_age) AS INT) AS avg_house_age,
        |        checkid
        |    FROM area_classified
        |    GROUP BY area_range, checkid
        |),
        |total AS (
        |    SELECT CAST(SUM(house_count) AS DOUBLE) AS total_count FROM stats
        |)
        |SELECT
        |    s.area_range,
        |    s.house_count,
        |    s.avg_price_per_sqm,
        |    s.min_price,
        |    s.max_price,
        |    s.median_price,
        |    s.price_variance,
        |    s.price_stddev,
        |    s.avg_house_age,
        |    DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS load_date,
        |    CASE
        |        WHEN s.avg_price_per_sqm < 8000  THEN '低价位'
        |        WHEN s.avg_price_per_sqm < 15000 THEN '中等价位'
        |        WHEN s.avg_price_per_sqm < 25000 THEN '中高价位'
        |        ELSE '高价位'
        |    END AS price_level,
        |    CAST(ROUND(s.house_count * 100.0 / t.total_count, 2) AS DECIMAL(5,2)) AS area_ratio,
        |    s.checkid,
        |    DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS pt_date
        |FROM stats s
        |CROSS JOIN total t
        |WHERE s.area_range != '未知'
        |ORDER BY s.area_range, s.checkid
      """.stripMargin

    println("========== 执行面积区间分析SQL ==========")
    println(analysisSQL)

    val analysisDF = spark.sql(analysisSQL)

    println("========== 分析结果 Schema ==========")
    analysisDF.printSchema()
    println("========== 分析结果预览（全部） ==========")
    analysisDF.show(50, truncate = false)

    // ==================== 第3步：将分析结果写入 area_price_analysis 表 ====================
    analysisDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "area_price_analysis", connProps)
    println("========== 分析结果已写入 area_price_analysis 表 ==========")

    spark.stop()
  }
}
