package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 分析功能1: Steam 游戏市场趋势分析
 *
 * 分析目标:
 * - 历年游戏发布趋势统计（按年份统计发布数量）
 * - 各年份游戏拥有量分布变化
 * - 游戏生命周期分析（发布年份与平均游玩时长的关系）
 * - 同比增长率（YoY）计算
 *
 * 使用字段: release_year, owners_numeric, Price, Average_playtime_forever, Recommendations
 */
object Analysis1 {

  /**
   * 执行分析
   *
   * @param spark SparkSession
   * @param df    清洗后的 Steam 数据
   * @return 分析结果 DataFrame (年份, 发布数, 平均拥有量, 平均价格, 平均游玩时长, YoY增长率)
   */
  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A1] 市场趋势分析 - 开始...")

    // Step 1: 按年份聚合
    val yearlyStats = aggregateByYear(df)

    // Step 2: 计算同比增长率
    val withYoY = calculateYoY(yearlyStats)

    // Step 3: 按年份排序
    val result = withYoY
      .orderBy($"year".asc)
      .select(
        $"year",
        $"release_count",
        round($"avg_owners", 0).as("avg_owners"),
        round($"avg_price", 2).as("avg_price"),
        round($"avg_playtime", 0).as("avg_playtime"),
        round($"total_recommendations", 0).as("total_recommendations"),
        round($"yoy_growth_pct", 2).as("yoy_growth_pct"),
        $"yoy_growth_category"
      )

    println(s"[A1] 市场趋势分析 - 完成 (${result.count()} 个年份)")

    // 打印概要
    println("[A1] 年度统计概览:")
    result.show(10, truncate = false)

    result
  }

  /**
   * 按年份聚合统计
   */
  private def aggregateByYear(df: DataFrame): DataFrame = {
    df
      .groupBy($"release_year".as("year"))
      .agg(
        count("*").as("release_count"),
        avg($"owners_numeric").as("avg_owners"),
        avg($"Price").as("avg_price"),
        avg($"Average_playtime_forever").as("avg_playtime"),
        sum($"Recommendations").as("total_recommendations")
      )
  }

  /**
   * 同比增长率计算（窗口函数）
   */
  private def calculateYoY(df: DataFrame): DataFrame = {
    val windowSpec = Window.orderBy($"year".asc)

    df
      .withColumn("prev_year_count",
        lag($"release_count", 1).over(windowSpec))
      .withColumn("yoy_growth_pct",
        when($"prev_year_count".isNull || $"prev_year_count" === 0, 0.0)
          .otherwise(round(($"release_count" - $"prev_year_count") / $"prev_year_count" * 100, 2))
      )
      .withColumn("yoy_growth_category",
        when($"yoy_growth_pct" > 20, "高速增长")
          .when($"yoy_growth_pct" > 5, "稳定增长")
          .when($"yoy_growth_pct" > -5, "基本持平")
          .when($"yoy_growth_pct" > -20, "缓慢下降")
          .otherwise("快速下降")
      )
      .drop("prev_year_count")
  }
}
