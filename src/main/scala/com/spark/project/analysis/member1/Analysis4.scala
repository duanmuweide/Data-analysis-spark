package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 分析功能4: 开发商与发行商生态分析
 */
object Analysis4 {

  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A4] 开发商与发行商生态分析 - 开始...")

    val devRanking = developerRanking(spark, df)
    println(s"[A4] 开发商 Top-15:")
    devRanking.show(15)

    val pubRanking = publisherRanking(spark, df)

    val hhiDev = calculateHHI(devRanking)
    val hhiPub = calculateHHI(pubRanking)
    println(s"[A4] 开发商 HHI: $hhiDev (${interpretHHI(hhiDev)})")
    println(s"[A4] 发行商 HHI: $hhiPub (${interpretHHI(hhiPub)})")

    val platformStats = platformSupportAnalysis(spark, df)
    platformStats.show(10)

    val devClass = developerClassification(spark, df)
    devClass.show(10)

    println(s"[A4] 完成")

    devRanking.withColumn("market_structure", lit(interpretHHI(hhiDev)))
  }

  private def developerRanking(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val totalOwners = df.agg(sum("owners_numeric")).first().getLong(0)

    df.groupBy($"Developers".as("developer"))
      .agg(
        count("*").as("game_count"),
        sum($"owners_numeric").as("total_owners"),
        round(avg($"Price"), 2).as("avg_price"),
        round(avg($"Metacritic_score"), 1).as("avg_metacritic"),
        countDistinct("Genres").as("genre_diversity")
      )
      .withColumn("market_share_pct", round($"total_owners" / totalOwners * 100, 4))
      .orderBy($"game_count".desc)
      .limit(50)
  }

  private def publisherRanking(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val totalOwners = df.agg(sum("owners_numeric")).first().getLong(0)

    df.groupBy($"Publishers".as("publisher"))
      .agg(
        count("*").as("game_count"),
        sum($"owners_numeric").as("total_owners"),
        round(avg($"Price"), 2).as("avg_price")
      )
      .withColumn("market_share_pct", round($"total_owners" / totalOwners * 100, 4))
      .orderBy($"game_count".desc)
      .limit(50)
  }

  private def calculateHHI(rankingDF: DataFrame): Double = {
    import rankingDF.sparkSession.implicits._
    val top100 = rankingDF.select("market_share_pct").limit(100).as[Double].collect()
    math.round(top100.map(share => share * share).sum)
  }

  private def interpretHHI(hhi: Double): String = {
    if (hhi < 1000) "分散市场"
    else if (hhi < 2500) "适度集中"
    else "高度集中"
  }

  private def platformSupportAnalysis(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df
      .withColumn("platform_count",
        when(upper($"Windows") === "TRUE" || $"Windows" === "1", 1).otherwise(0) +
        when(upper($"Mac") === "TRUE" || $"Mac" === "1", 1).otherwise(0) +
        when(upper($"Linux") === "TRUE" || $"Linux" === "1", 1).otherwise(0)
      )
      .withColumn("platform_category",
        when($"platform_count" >= 3, "三平台")
          .when($"platform_count" === 2, "双平台")
          .when($"platform_count" === 1, "单平台(Windows)")
          .otherwise("其他")
      )
      .groupBy("platform_category")
      .agg(count("*").as("game_count"), round(avg($"owners_numeric"), 0).as("avg_owners"))
      .orderBy($"game_count".desc)
  }

  private def developerClassification(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val counts = df.groupBy("Developers").agg(
      count("*").as("game_count"),
      sum($"owners_numeric").as("total_owners"),
      round(avg($"Price"), 2).as("avg_price"),
      round(avg($"Metacritic_score"), 1).as("avg_metacritic")
    )
    counts.withColumn("developer_category",
      when($"game_count" === 1, "独立开发者")
        .when($"game_count" >= 2 && $"game_count" <= 10, "小型厂商")
        .when($"game_count" >= 11 && $"game_count" <= 50, "中型厂商")
        .otherwise("大型厂商")
    )
    .groupBy("developer_category").agg(
      count("*").as("developer_count"),
      sum("game_count").as("total_games"),
      round(avg("avg_price"), 2).as("avg_game_price"),
      round(avg("avg_metacritic"), 1).as("avg_metacritic")
    )
    .orderBy($"developer_count".desc)
  }
}
