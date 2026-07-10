package com.qdu.jdbc

import org.apache.spark.sql.SaveMode

import java.util.Properties

object YearAnalysis {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("YearAnalysis")
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

    // ==================== 第2步：按建造年份区间分析房屋数据 ====================
    // 分析维度说明：
    //   - 年份区间划分：1950-1970 / 1970-1990 / 1990-2000 / 2000-2010 / 2010-2020
    //   - 分析指标：房屋数量、电梯数量、小/中/大户型数量、精装/简装/毛坯数量
    //   - 户型判断：根据 layout 字段中"X室"的数量来归类
    //   - 装修判断：根据 decoration 字段内容归类

    val analysisSQL =
      """
        |WITH year_classified AS (
        |    SELECT
        |        build_year,
        |        elevator_int,
        |        layout,
        |        decoration,
        |        checkid,
        |        CASE
        |            WHEN build_year >= 1950 AND build_year < 1970 THEN '1950-1970'
        |            WHEN build_year >= 1970 AND build_year < 1990 THEN '1970-1990'
        |            WHEN build_year >= 1990 AND build_year < 2000 THEN '1990-2000'
        |            WHEN build_year >= 2000 AND build_year < 2010 THEN '2000-2010'
        |            WHEN build_year >= 2010 AND build_year <= 2020 THEN '2010-2020'
        |            ELSE '其他'
        |        END AS year_range,
        |        -- 户型分类：提取"X室"中的数字来判断
        |        CASE
        |            WHEN layout RLIKE '^[0-9]室' THEN
        |                CAST(regexp_extract(layout, '^([0-9])室', 1) AS INT)
        |            ELSE -1
        |        END AS room_count
        |    FROM house_info_clean_checkid
        |    WHERE build_year IS NOT NULL
        |      AND build_year >= 1950
        |      AND build_year <= 2020
        |)
        |SELECT
        |    year_range,
        |    COUNT(*) AS house_count,
        |    SUM(elevator_int) AS elevator_count,
        |    SUM(CASE WHEN room_count >= 1 AND room_count <= 2 THEN 1 ELSE 0 END) AS small_layout_count,
        |    SUM(CASE WHEN room_count >= 3 AND room_count <= 4 THEN 1 ELSE 0 END) AS medium_layout_count,
        |    SUM(CASE WHEN room_count >= 5 OR room_count = -1 THEN 1 ELSE 0 END) AS large_layout_count,
        |    SUM(CASE WHEN decoration = '精装' THEN 1 ELSE 0 END) AS premium_decoration_count,
        |    SUM(CASE WHEN decoration = '简装' THEN 1 ELSE 0 END) AS simple_decoration_count,
        |    SUM(CASE WHEN decoration = '毛坯' THEN 1 ELSE 0 END) AS rough_decoration_count,
        |    NOW() AS analysis_time,
        |    checkid,
        |    DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS pt_date
        |FROM year_classified
        |WHERE year_range != '其他'
        |GROUP BY year_range, checkid
        |ORDER BY year_range, checkid
      """.stripMargin

    println("========== 执行年份区间分析SQL ==========")
    println(analysisSQL)

    val analysisDF = spark.sql(analysisSQL)

    println("========== 分析结果 Schema ==========")
    analysisDF.printSchema()
    println("========== 分析结果预览（全部） ==========")
    analysisDF.show(100, truncate = false)

    // ==================== 第3步：将分析结果写入 house_year_analysis 表 ====================
    analysisDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "house_year_analysis", connProps)
    println("========== 分析结果已写入 house_year_analysis 表 ==========")

    spark.stop()
  }
}
