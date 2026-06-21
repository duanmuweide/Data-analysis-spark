package com.spark.project.common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Properties

/**
 * 通用工具类 — Steam 游戏数据处理
 *
 * 提供项目中复用的数据处理函数：
 * - XLSX/CSV 数据读取
 * - 数据清洗（处理 Estimated owners 范围、JSON数组字段等）
 * - 结果保存
 */
object Utils {

  /**
   * Steam 数据字段定义（对应 games.xlsx 的列）
   */
  val SCHEMA: StructType = StructType(Array(
    StructField("AppID",         LongType,    nullable = true),
    StructField("Name",          StringType,  nullable = true),
    StructField("Release_date",  StringType,  nullable = true),
    StructField("Estimated_owners", StringType, nullable = true),
    StructField("Peak_CCU",     IntegerType, nullable = true),
    StructField("Price",         DoubleType,  nullable = true),
    StructField("Discount",      IntegerType, nullable = true),
    StructField("Positive",      IntegerType, nullable = true),
    StructField("Negative",      IntegerType, nullable = true),
    StructField("Metacritic_score", IntegerType, nullable = true),
    StructField("Categories",    StringType,  nullable = true),
    StructField("Genres",        StringType,  nullable = true),
    StructField("Tags",          StringType,  nullable = true),
    StructField("Developers",    StringType,  nullable = true),
    StructField("Publishers",    StringType,  nullable = true),
    StructField("Windows",       StringType,  nullable = true),
    StructField("Mac",           StringType,  nullable = true),
    StructField("Linux",         StringType,  nullable = true),
    StructField("Average_playtime_forever", LongType, nullable = true),
    StructField("Supported_languages", StringType, nullable = true),
    StructField("Recommendations", LongType, nullable = true)
  ))

  // ===================================================================
  // 数据读取
  // ===================================================================

  /**
   * 读取 Steam 游戏数据（自动检测 XLSX / CSV 格式）
   *
   * 源文件格式说明：
   * - 虽然扩展名是 .csv，但实际可能是 XLSX 格式（Excel）
   * - 优先尝试 spark-excel 读取，失败则回退到 CSV
   *
   * @param spark    SparkSession
   * @param path     文件路径
   * @param isSample 是否为样例数据（开发测试用）
   * @return DataFrame
   */
  def readSteamData(spark: SparkSession, path: String, isSample: Boolean = false): DataFrame = {
    val actualPath = if (isSample) {
      Config.STEAM_SAMPLE_FILE.replace("games-example.csv", path.substring(path.lastIndexOf("/") + 1))
      path // 使用传入的完整路径
    } else {
      path
    }

    println(s"[Utils] 读取数据: $actualPath")

    // 尝试用 spark-excel 读取（文件可能是 XLSX 格式）
    try {
      val df = spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dataAddress", "'Sheet1'!") // Excel 工作表
        .option("treatEmptyValuesAsNulls", "true")
        .option("addColorColumns", "false")
        .load(actualPath)

      println(s"[Utils] spark-excel 读取成功: ${df.count()} 行, ${df.columns.length} 列")
      println(s"[Utils] 列名: ${df.columns.mkString(", ")}")
      df
    } catch {
      case e: Exception =>
        println(s"[Utils] spark-excel 读取失败 (${e.getMessage})，尝试 CSV 格式...")
        // 回退到 CSV 读取
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("multiLine", "true")
          .option("escape", "\"")
          .csv(actualPath)
    }
  }

  // ===================================================================
  // 数据清洗
  // ===================================================================

  /**
   * 解析 Estimated owners 范围字段
   *
   * 原始值示例: "0 - 20000", "20000 - 50000", "100000 - 200000"
   * 返回该范围的中点值作为估计拥有量
   *
   * @param ownersStr 原始范围字符串
   * @return 估计拥有量（中点值）
   */
  def parseEstimatedOwners(ownersStr: String): Long = {
    if (ownersStr == null || ownersStr.trim.isEmpty) return 0L
    val parts = ownersStr.split("-").map(_.trim)
    if (parts.length == 2) {
      try {
        val low = parts(0).toLong
        val high = parts(1).toLong
        (low + high) / 2
      } catch {
        case _: NumberFormatException => 0L
      }
    } else {
      try {
        ownersStr.trim.toLong
      } catch {
        case _: NumberFormatException => 0L
      }
    }
  }

  // Spark SQL UDF 注册函数名
  val ESTIMATED_OWNERS_UDF_NAME = "parse_estimated_owners"

  /**
   * 解析 JSON 数组字符串字段
   *
   * 原始值示例: "['English', 'Simplified Chinese', 'Japanese']"
   *            "['Single-player', 'Steam Achievements']"
   *            "['Action', 'Indie']"
   *
   * @param jsonArrayStr JSON数组格式字符串
   * @return 拆分后的数组
   */
  def parseArrayField(jsonArrayStr: String): Array[String] = {
    if (jsonArrayStr == null || jsonArrayStr.trim.isEmpty) return Array.empty[String]
    // 清理方括号和引号，按逗号分割
    jsonArrayStr
      .replaceAll("^\\[|\\]$", "") // 去掉首尾方括号
      .replaceAll("['\"]", "")     // 去掉引号
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  /**
   * 数据清洗：Steam 游戏数据专用
   *
   * 清洗步骤:
   * 1. 去除关键列为null的行 (AppID, Name)
   * 2. 去重 (基于 AppID)
   * 3. 解析 Estimated owners → 数值
   * 4. Price 处理（免费游戏 = 0）
   * 5. Release date 解析为年份
   *
   * @param df 原始 DataFrame
   * @return 清洗后的 DataFrame
   */
  def cleanSteamData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    // 注册 UDF
    val parseOwnersUdf = udf(parseEstimatedOwners _)
    val parseArrayUdf = udf(parseArrayField _)

    // 统一列名（Excel 读取可能带空格/特殊字符）
    val renamed = normalizeColumnNames(df)

    val cleaned = renamed
      // Step 1: 去重 + 去空
      .filter($"AppID".isNotNull && $"Name".isNotNull && $"Name" =!= "")
      .dropDuplicates("AppID")
      // Step 2: 解析 Estimated owners
      .withColumn("owners_numeric", parseOwnersUdf($"Estimated_owners"))
      // Step 3: Price 处理 null → 0
      .withColumn("Price", when($"Price".isNull, 0.0).otherwise($"Price"))
      // Step 4: 解析 Release date → 年份
      .withColumn("release_year",
        when($"Release_date".isNotNull,
          substring_index($"Release_date", "-", 1)   // "2021-05-15" → "2021"
            .cast(IntegerType)
        ).otherwise(0)
      )
      // Step 5: 过滤无效发行年份
      .filter($"release_year" >= 1980 && $"release_year" <= 2026)

    println(s"[Utils] 数据清洗完成: ${cleaned.count()} 行 (去重、去空、年份过滤后)")

    cleaned
  }

  /**
   * 统一列名：去除空格，替换特殊字符
   */
  private def normalizeColumnNames(df: DataFrame): DataFrame = {
    var result = df
    df.columns.foreach { colName =>
      val normalized = colName.trim
        .replace(" ", "_")
        .replace(".", "_")
      if (normalized != colName) {
        result = result.withColumnRenamed(colName, normalized)
      }
    }
    result
  }

  // ===================================================================
  // 结果保存
  // ===================================================================

  /**
   * 结果保存为 CSV
   */
  def saveAsCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
    println(s"[Utils] CSV 已保存: $path")
  }

  /**
   * 结果保存到 MySQL 数据库
   */
  def saveToDatabase(df: DataFrame, tableName: String): Unit = {
    val props = new Properties()
    props.setProperty("user", Config.DB_USER)
    props.setProperty("password", Config.DB_PASSWORD)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    df
      .coalesce(1) // 减少并发写入压力
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(Config.JDBC_URL, tableName, props)

    println(s"[Utils] 数据已写入 MySQL: $tableName (${df.count()} 行)")
  }

  /**
   * 结果追加到 MySQL 数据库
   */
  def appendToDatabase(df: DataFrame, tableName: String): Unit = {
    val props = new Properties()
    props.setProperty("user", Config.DB_USER)
    props.setProperty("password", Config.DB_PASSWORD)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    df
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .jdbc(Config.JDBC_URL, tableName, props)

    println(s"[Utils] 数据已追加到 MySQL: $tableName")
  }

  /**
   * 性能计时工具
   */
  def time[T](block: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val result = block
    val duration = System.currentTimeMillis() - start
    (result, duration)
  }
}
