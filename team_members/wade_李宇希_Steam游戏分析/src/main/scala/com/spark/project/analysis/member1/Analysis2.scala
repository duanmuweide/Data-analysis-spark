package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 分析功能2: 游戏定价与用户评价关联分析
 */
object Analysis2 {

  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A2] 定价与评价关联分析 - 开始...")

    val withBuckets = bucketPrice(spark, df)
    val withPositiveRate = calculatePositiveRate(spark, withBuckets)

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

    val totalGames = aggregated.agg(sum("game_count")).first().getLong(0)
    val totalOwnersAll = aggregated.agg(sum("total_owners")).first().getDouble(0).toLong

    val withPct = aggregated
      .withColumn("game_pct", round($"game_count" / totalGames * 100, 2))
      .withColumn("owners_share_pct",
        round($"total_owners" / totalOwnersAll * 100, 2))

    val bestValueGames = findBestValueGames(spark, withPositiveRate)

    println(s"[A2] 定价分布 (共 $totalGames 款游戏):")
    withPct.select("price_bucket", "game_count", "game_pct", "avg_positive_rate_pct", "avg_metacritic_score")
      .show(10)

    println(s"[A2] 高性价比游戏 Top-20:")
    bestValueGames.show(20)

    println(s"[A2] 完成")
    withPct
  }

  private def bucketPrice(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.withColumn("price_bucket",
      when($"Price" === 0.0, "Free")
        .when($"Price" > 0 && $"Price" <= 4.99, "0.01-4.99")
        .when($"Price" >= 5 && $"Price" <= 9.99, "5-9.99")
        .when($"Price" >= 10 && $"Price" <= 19.99, "10-19.99")
        .when($"Price" >= 20 && $"Price" <= 49.99, "20-49.99")
        .otherwise("50+")
    )
  }

  private def calculatePositiveRate(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df
      .withColumn("total_reviews", $"Positive" + $"Negative")
      .withColumn("positive_rate",
        when($"total_reviews" > 0, $"Positive" / $"total_reviews")
          .otherwise(0.5)
      )
  }

  private def findBestValueGames(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df
      .filter($"positive_rate" >= 0.85 &&
              $"Recommendations" >= 5000 &&
              $"Price" <= 19.99 &&
              $"total_reviews" >= 100)
      .orderBy($"positive_rate".desc, $"Recommendations".desc)
      .limit(20)
  }
}
