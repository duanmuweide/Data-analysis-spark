package com.spark.project.common

import org.apache.spark.sql.{DataFrame, SparkSession}

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
   * 读取 Steam 游戏数据（XLSX格式）
   *
   * 源文件为 Excel .xlsx 格式，需要 Spark 读取后正确处理：
   * - 自动推断 Schema
   * - Estimated owners 列是范围字符串 "0 - 20000"，需解析
   * - Supported languages / Genres / Tags 是 JSON 数组字符串
   *
   * @param spark    SparkSession
   * @param path     文件路径
   * @param isSample 是否为样例数据（开发测试用）
   * @return DataFrame
   */
  def readSteamData(spark: SparkSession, path: String, isSample: Boolean = false): DataFrame = {
    // TODO: 实现 XLSX 数据读取
    // 方案：
    // 1. 使用 spark.read.format("com.crealytics.spark.excel") 直接读取 .xlsx
    //    或 先用 Python 脚本将 .xlsx 转为 .csv 再读取
    // 2. 对于 390MB 大文件，建议先转为 CSV/Parquet
    // 3. 样例数据直接用 com.crealytics.spark.excel 读取
    ???
  }

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
    // TODO: 解析范围并返回中点
    // 处理特殊情况: "0 - 0" 返回 0
    ???
  }

  /**
   * 解析 JSON 数组字符串字段
   *
   * 原始值示例: "['English', 'Simplified Chinese', 'Japanese']"
   *            "['Single-player', 'Steam Achievements']"
   *
   * @param jsonArrayStr JSON数组格式字符串
   * @return 拆分后的数组
   */
  def parseArrayField(jsonArrayStr: String): Array[String] = {
    // TODO: 清理并拆分数组字段
    ???
  }

  /**
   * 数据清洗：Steam 游戏数据专用
   *
   * 清洗步骤:
   * 1. 去除关键列为null的行 (AppID, Name)
   * 2. 去重 (基于 AppID)
   * 3. 解析 Estimated owners → 数值
   * 4. Price 处理（免费游戏 = 0）
   * 5. Release date 解析为日期类型
   *
   * @param df 原始 DataFrame
   * @return 清洗后的 DataFrame
   */
  def cleanSteamData(df: DataFrame): DataFrame = {
    // TODO: 实现 Steam 数据清洗
    ???
  }

  /**
   * 结果保存为 CSV
   */
  def saveAsCsv(df: DataFrame, path: String): Unit = {
    // TODO: 保存为 CSV
    ???
  }

  /**
   * 结果保存到数据库
   */
  def saveToDatabase(df: DataFrame, tableName: String): Unit = {
    // TODO: JDBC 写入
    ???
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
