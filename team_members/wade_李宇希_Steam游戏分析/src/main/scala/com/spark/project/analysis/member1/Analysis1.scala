package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 分析功能1: Steam 游戏市场趋势分析
 */
object Analysis1 {

  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A1] 市场趋势分析 - 开始...")

    val yearlyStats = aggregateByYear(spark, df)
    val withYoY = calculateYoY(spark, yearlyStats)

    val result = withYoY
      .orderBy($"year".asc)
      .select(
        $"year", $"release_count",
        round($"avg_owners", 0).as("avg_owners"),
        round($"avg_price", 2).as("avg_price"),
        round($"avg_playtime", 0).as("avg_playtime"),
        round($"total_recommendations", 0).as("total_recommendations"),
        round($"yoy_growth_pct", 2).as("yoy_growth_pct"),
        $"yoy_growth_category"
      )

    println(s"[A1] 完成 (${result.count()} 个年份)")
    result.show(10)
    result
  }

  private def aggregateByYear(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.groupBy($"release_year".as("year"))
      .agg(
        count("*").as("release_count"),
        avg($"owners_numeric").as("avg_owners"),
        avg($"Price").as("avg_price"),
        avg($"Average_playtime_forever").as("avg_playtime"),
        sum($"Recommendations").as("total_recommendations")
      )
  }

  private def calculateYoY(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val windowSpec = Window.orderBy($"year".asc)

    df
      .withColumn("prev_year_count", lag($"release_count", 1).over(windowSpec))
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
  }
}
