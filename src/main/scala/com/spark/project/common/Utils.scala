package com.spark.project.common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Properties

/**
 * 通用工具类 — Steam 游戏数据处理
 *
 * 提供项目中复用的数据处理函数：
 * - CSV 数据读取（先用 Python 把 XLSX 转为 CSV）
 * - 数据清洗（处理 Estimated owners 范围、JSON数组字段等）
 * - 结果保存
 */
object Utils {

  // ===================================================================
  // 数据读取
  // ===================================================================

  /**
   * 读取 Steam 游戏数据（CSV 格式）
   *
   * 注意: 原始数据为 XLSX，需先转换为 CSV:
   *   python scripts/convert_xlsx_to_csv.py
   */
  def readSteamData(spark: SparkSession, path: String, isSample: Boolean = false): DataFrame = {
    println(s"[Utils] 读取 CSV: $path")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .csv(path)

    println(s"[Utils] 列数: ${df.columns.length}")
    df
  }

  // ===================================================================
  // 数据清洗
  // ===================================================================

  /**
   * 解析 Estimated owners 范围 → 中点值
   * 例: "0 - 20000" → 10000
   */
  def parseEstimatedOwners(ownersStr: String): Long = {
    if (ownersStr == null || ownersStr.trim.isEmpty) return 0L
    val parts = ownersStr.split("-").map(_.trim)
    if (parts.length == 2) {
      try { (parts(0).toLong + parts(1).toLong) / 2 }
      catch { case _: NumberFormatException => 0L }
    } else {
      try { ownersStr.trim.toLong }
      catch { case _: NumberFormatException => 0L }
    }
  }

  /**
   * 解析 JSON 数组字符串 → Array[String]
   * 例: "['Action','Indie']" → Array("Action", "Indie")
   */
  def parseArrayField(jsonArrayStr: String): Array[String] = {
    if (jsonArrayStr == null || jsonArrayStr.trim.isEmpty) return Array.empty[String]
    jsonArrayStr
      .replaceAll("^\\[|\\]$", "")
      .replaceAll("['\"]", "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  /**
   * 完整数据清洗流水线
   */
  def cleanSteamData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val parseOwnersUdf = udf(parseEstimatedOwners _)

    val renamed = normalizeColumnNames(df)

    val cleaned = renamed
      .filter($"AppID".isNotNull && $"Name".isNotNull && $"Name" =!= "")
      .dropDuplicates("AppID")
      .withColumn("owners_numeric", parseOwnersUdf($"Estimated_owners"))
      .withColumn("Price", when($"Price".isNull, 0.0).otherwise($"Price"))
      .withColumn("release_year",
        when($"Release_date".isNotNull,
          substring_index($"Release_date", "-", 1).cast(IntegerType)
        ).otherwise(0)
      )
      .filter($"release_year" >= 1980 && $"release_year" <= 2026)

    println(s"[Utils] 清洗完成: ${cleaned.count()} 行")
    cleaned
  }

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
    val props = new Properties()
    props.setProperty("user", Config.DB_USER)
    props.setProperty("password", Config.DB_PASSWORD)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .jdbc(Config.JDBC_URL, tableName, props)

    println(s"[Utils] 写入 MySQL: $tableName")
  }

  def time[T](block: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    (result, System.currentTimeMillis() - start)
  }
}
