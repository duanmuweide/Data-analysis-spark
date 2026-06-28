package com.qdu.jdbc

import org.apache.spark.sql.SaveMode

import java.util.Properties

object DistrictAnalysis {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("DistrictAnalysis")
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

    // ==================== 第2步：按市区维度分析房价数据 ====================
    // 分析维度说明：
    //   - 按 district（市区）分组
    //   - 分析指标：平均房价、房屋数量、最低/最高/中位数单价、
    //               价格方差/标准差、平均房龄、平均面积
    //   - 按 checkid（数据批次ID）分组，保留批次信息

    val analysisSQL =
      """
        |SELECT
        |    district,
        |    CAST(AVG(price_per_sqm) AS INT) AS avg_price_per_sqm,
        |    COUNT(*) AS house_count,
        |    CAST(MIN(price_per_sqm) AS INT) AS min_price,
        |    CAST(MAX(price_per_sqm) AS INT) AS max_price,
        |    CAST(percentile_approx(CAST(price_per_sqm AS DOUBLE), 0.5) AS INT) AS median_price,
        |    CAST(VARIANCE(price_per_sqm) AS INT) AS price_variance,
        |    CAST(STDDEV(price_per_sqm) AS INT) AS std_price,
        |    CAST(AVG(house_age) AS INT) AS avg_house_age,
        |    CAST(AVG(area) AS INT) AS avg_area,
        |    checkid,
        |    DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS load_date
        |FROM house_info_clean_checkid
        |WHERE district IS NOT NULL
        |  AND district != ''
        |  AND price_per_sqm IS NOT NULL
        |  AND price_per_sqm > 0
        |  AND area IS NOT NULL
        |  AND area > 0
        |GROUP BY district, checkid
        |ORDER BY district, checkid
      """.stripMargin

    println("========== 执行市区维度分析SQL ==========")
    println(analysisSQL)

    val analysisDF = spark.sql(analysisSQL)

    println("========== 分析结果 Schema ==========")
    analysisDF.printSchema()
    println("========== 分析结果预览（全部） ==========")
    analysisDF.show(100, truncate = false)

    // ==================== 第3步：将分析结果写入 district_house_price_analysis 表 ====================
    analysisDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "district_house_price_analysis", connProps)
    println("========== 分析结果已写入 district_house_price_analysis 表 ==========")

    spark.stop()
  }
}
