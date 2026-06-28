package com.spark.project.common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.util.Properties

/**
 * 通用工具类 — Steam 游戏数据处理
 *
 * 提供项目中复用的数据处理函数：
 * - CSV 数据读取（header + inferSchema，自适应CSV结构变化）
 * - 数据清洗（纯 Spark SQL 表达式，避免 UDF 触发 ClosureCleaner）
 * - 结果保存
 */
object Utils {

  // ===================================================================
  // 数据读取
  // ===================================================================

  /**
   * 读取 Steam 游戏数据（CSV 格式）— inferSchema 自动适配 CSV 结构
   */
  def readSteamData(spark: SparkSession, path: String, isSample: Boolean = false): DataFrame = {
    println(s"[Utils] 读取 CSV: $path")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .csv(path)

    println(s"[Utils] 列数: ${df.columns.length}, 列名: ${df.columns.mkString(", ")}")
    df
  }

  // ===================================================================
  // 数据清洗（全部用 Spark SQL 内置函数，无 UDF，不触发 ClosureCleaner）
  // ===================================================================

  /**
   * 完整数据清洗流水线
   */
  def cleanSteamData(df: DataFrame): DataFrame = {

    // 规范化列名
    val renamed = normalizeColumnNames(df)

    // 解析 Estimated_owners: "0 - 20000" → 取中点值 (0+20000)/2 = 10000
    val cleaned = renamed
      .filter(col("AppID").isNotNull && col("Name").isNotNull && col("Name") =!= "")
      .dropDuplicates("AppID")
      .withColumn("owners_numeric",
        // 纯 Spark SQL: 取首尾数字算均值
        (regexp_extract(col("Estimated_owners"), "^([0-9]+)", 1).cast("long") +
          regexp_extract(col("Estimated_owners"), "([0-9]+)$", 1).cast("long")) / 2
      )
      .withColumn("Price", when(col("Price").isNull, 0.0).otherwise(col("Price")))
      .withColumn("release_year",
        when(col("Release_date").isNotNull,
          // 从各种日期格式中提取首个4位年份（兼容 "2023-01-15", "Oct 21, 2008", "2008" 等）
          regexp_extract(col("Release_date"), "(\\d{4})", 1).cast(IntegerType)
        ).otherwise(0)
      )
      .filter(col("release_year") >= 1980 && col("release_year") <= 2026)

    cleaned
  }

  // ===================================================================
  // 辅助函数
  // ===================================================================

  private def normalizeColumnNames(df: DataFrame): DataFrame = {
    var result = df
    df.columns.foreach { colName =>
      val normalized = colName.trim.replace(" ", "_").replace(".", "_")
      if (normalized != colName) result = result.withColumnRenamed(colName, normalized)
    }
    result
  }

  // ===================================================================
  // 结果保存
  // ===================================================================

  def saveAsCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(path)
  }

  def saveToDatabase(df: DataFrame, tableName: String): Unit = {
    try {
      val props = new Properties()
      props.setProperty("user", Config.DB_USER)
      props.setProperty("password", Config.DB_PASSWORD)
      props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

      df.coalesce(1).write.mode(SaveMode.Overwrite)
        .jdbc(Config.JDBC_URL, tableName, props)

      println(s"[Utils] 写入 MySQL: $tableName (${df.count()} 行)")
    } catch {
      case e: Exception =>
        println(s"[Utils] ⚠ MySQL 写入失败 ($tableName): ${e.getMessage}")
        println(s"[Utils] ⚠ 请检查 MySQL 服务是否启动、密码是否正确: ${Config.JDBC_URL}")
    }
  }

  def time[T](block: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    (result, System.currentTimeMillis() - start)
  }
}
