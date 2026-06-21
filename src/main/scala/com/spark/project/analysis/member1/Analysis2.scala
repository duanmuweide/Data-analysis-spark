package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 分析功能2: 游戏定价与用户评价关联分析
 *
 * 分析目标:
 * - 价格区间与用户评分的关系
 * - 免费 vs 付费游戏评价差异对比
 * - 不同价格区间的游戏数量分布
 * - 高性价比游戏 Top-20 推荐
 *
 * 使用字段: Price, Positive, Negative, Metacritic_score, owners_numeric, Recommendations
 */
object Analysis2 {

  /**
   * 执行分析
   *
   * @param spark SparkSession
   * @param df    清洗后的 Steam 数据
   * @return 分析结果 DataFrame
   */
  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A2] 定价与评价关联分析 - 开始...")

    // Step 1: 价格分桶 + 计算好评率
    val withBuckets = bucketPrice(df)
    val withPositiveRate = calculatePositiveRate(withBuckets)

    // Step 2: 按价格区间聚合
    val aggregated = withPositiveRate
      .groupBy($"price_bucket")
      .agg(
        count("*").as("game_count"),
        round(avg($"positive_rate") * 100, 2).as("avg_positive_rate_pct"),
        round(avg($"Metacritic_score"), 1).as("avg_metacritic_score"),
        round(avg($"Recommendations"), 0).as("avg_recommendations"),
        round(avg($"owners_numeric"), 0).as("avg_owners"),
        round(avg($"Price"), 2).as("avg_price_in_bucket"),
        sum($"owners_numeric").as("total_owners")
      )
      .orderBy($"game_count".desc)

    // Step 3: 计算每个价格区间的占比
    val totalGames = aggregated.agg(sum("game_count")).first().getLong(0)
    val withPct = aggregated
      .withColumn("game_pct", round($"game_count" / totalGames * 100, 2))
      .withColumn("owners_share_pct",
        round($"total_owners" / aggregated.agg(sum("total_owners")).first().getLong(0) * 100, 2)
      )

    // Step 4: 高性价比游戏 Top-20
    val bestValueGames = findBestValueGames(withPositiveRate)

    println(s"[A2] 定价分布 (共 $totalGames 款游戏):")
    withPct.select("price_bucket", "game_count", "game_pct", "avg_positive_rate_pct", "avg_metacritic_score")
      .show(10, truncate = false)

    println(s"[A2] 高性价比游戏 Top-20:")
    bestValueGames.select("AppID", "Name", "Price", "positive_rate", "Recommendations", "owners_numeric")
      .show(20, truncate = false)

    println(s"[A2] 定价与评价分析 - 完成")

    // 返回聚合结果
    withPct
  }

  /**
   * 价格分桶
   */
  private def bucketPrice(df: DataFrame): DataFrame = {
    df.withColumn("price_bucket",
      when($"Price" === 0.0, "Free")
        .when($"Price" > 0 && $"Price" <= 4.99, "$0.01-4.99")
        .when($"Price" >= 5 && $"Price" <= 9.99, "$5-9.99")
        .when($"Price" >= 10 && $"Price" <= 19.99, "$10-19.99")
        .when($"Price" >= 20 && $"Price" <= 49.99, "$20-49.99")
        .otherwise("$50+")
    )
  }

  /**
   * 好评率计算
   */
  private def calculatePositiveRate(df: DataFrame): DataFrame = {
    df
      .withColumn("total_reviews", $"Positive" + $"Negative")
      .withColumn("positive_rate",
        when($"total_reviews" > 0,
          $"Positive" / $"total_reviews"
        ).otherwise(0.5) // 无评价默认 50%
      )
  }

  /**
   * 高性价比游戏筛选
   */
  private def findBestValueGames(df: DataFrame): DataFrame = {
    df
      .filter(
        $"positive_rate" >= 0.85 &&           // 好评率 > 85%
        $"Recommendations" >= 5000 &&         // 推荐数 > 5000
        $"Price" <= 19.99 &&                  // 价格 <= $19.99
        $"total_reviews" >= 100               // 评价总数 > 100
      )
      .orderBy($"positive_rate".desc, $"Recommendations".desc)
      .limit(20)
  }
}
