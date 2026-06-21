package com.spark.project.analysis.member1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 分析功能4: 开发商与发行商生态分析
 *
 * 分析目标:
 * - 开发商/发行商市场份额排名
 * - 市场集中度分析（CR4/CR10，赫芬达尔指数 HHI）
 * - 跨平台支持分析（Win/Mac/Linux 三平台支持率）
 * - 开发商类型画像（独立开发者 vs 大厂 vs 中等厂商）
 *
 * 使用字段: Developers, Publishers, owners_numeric, Windows, Mac, Linux, Genres, Price
 */
object Analysis4 {

  /**
   * 执行分析
   *
   * @param spark SparkSession
   * @param df    清洗后的 Steam 数据
   * @return 分析结果 DataFrame
   */
  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A4] 开发商与发行商生态分析 - 开始...")

    // Part 1: 开发商排名 Top-30
    println("[A4] Part 1: 开发商市场份额排名...")
    val devRanking = developerRanking(df)
    devRanking.show(15, truncate = false)

    // Part 2: 发行商排名 Top-30
    println("[A4] Part 2: 发行商市场份额排名...")
    val pubRanking = publisherRanking(df)

    // Part 3: 市场集中度 HHI
    println("[A4] Part 3: 市场集中度 HHI 计算...")
    val hhiDev = calculateHHI(devRanking)
    val hhiPub = calculateHHI(pubRanking)
    println(s"[A4] 开发商 HHI: $hhiDev (${interpretHHI(hhiDev)})")
    println(s"[A4] 发行商 HHI: $hhiPub (${interpretHHI(hhiPub)})")

    // Part 4: 跨平台分析
    println("[A4] Part 4: 跨平台支持分析...")
    val platformStats = platformSupportAnalysis(df)
    platformStats.show(10, truncate = false)

    // Part 5: 开发商分类
    println("[A4] Part 5: 开发商类型分类...")
    val devClass = developerClassification(df)
    devClass.show(10, truncate = false)

    println(s"[A4] 开发商生态分析 - 完成")

    // 返回开发商排名（主要结果）
    devRanking
      .withColumn("hhi", lit(hhiDev))
      .select(
        $"rank",
        $"developer",
        $"game_count",
        $"total_owners",
        round($"market_share_pct", 2).as("market_share_pct"),
        round($"avg_price", 2).as("avg_price"),
        $"platform_diversity",
        lit(hhiDev).as("market_hhi"),
        lit(interpretHHI(hhiDev)).as("market_structure")
      )
  }

  /**
   * 开发商排名 Top-50
   */
  private def developerRanking(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val totalOwners = df.agg(sum("owners_numeric")).first().getLong(0)

    df
      .groupBy($"Developers".as("developer"))
      .agg(
        count("*").as("game_count"),
        sum($"owners_numeric").as("total_owners"),
        round(avg($"Price"), 2).as("avg_price"),
        round(avg($"Metacritic_score"), 1).as("avg_metacritic"),
        countDistinct("Genres").as("genre_diversity")
      )
      .withColumn("market_share_pct",
        round($"total_owners" / totalOwners * 100, 4))
      .withColumn("platform_diversity",
        // 后续 join 添加，先占位
        lit("N/A").as("platform_diversity")
      )
      .orderBy($"game_count".desc)
      .limit(50)
      .withColumn("rank", monotonically_increasing_id() + 1)
      .select($"rank", $"developer", $"game_count", $"total_owners",
        $"market_share_pct", $"avg_price", $"avg_metacritic",
        $"genre_diversity", $"platform_diversity")
  }

  /**
   * 发行商排名 Top-50
   */
  private def publisherRanking(df: DataFrame): DataFrame = {
    val totalOwners = df.agg(sum("owners_numeric")).first().getLong(0)

    df
      .groupBy($"Publishers".as("publisher"))
      .agg(
        count("*").as("game_count"),
        sum($"owners_numeric").as("total_owners"),
        round(avg($"Price"), 2).as("avg_price")
      )
      .withColumn("market_share_pct",
        round($"total_owners" / totalOwners * 100, 4))
      .orderBy($"game_count".desc)
      .limit(50)
      .withColumn("rank", monotonically_increasing_id() + 1)
      .select($"rank", $"publisher", $"game_count", $"total_owners",
        $"market_share_pct", $"avg_price")
  }

  /**
   * 市场集中度 HHI 计算
   * HHI = Σ(每个厂商的市场份额%)^2
   * - HHI < 1000: 分散市场（竞争充分）
   * - 1000 ≤ HHI < 2500: 适度集中
   * - HHI ≥ 2500: 高度集中
   */
  private def calculateHHI(rankingDF: DataFrame): Double = {
    import rankingDF.sparkSession.implicits._

    // 取前 100 个厂商计算 HHI（其余长尾贡献极小）
    val top100 = rankingDF
      .select("market_share_pct")
      .limit(100)
      .as[Double]
      .collect()

    val hhi = top100.map(share => share * share).sum

    // 四舍五入到整数
    math.round(hhi)
  }

  private def interpretHHI(hhi: Double): String = {
    if (hhi < 1000) "分散市场（竞争充分）"
    else if (hhi < 2500) "适度集中市场"
    else "高度集中市场"
  }

  /**
   * 跨平台支持分析
   */
  private def platformSupportAnalysis(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    // 将 Windows/Mac/Linux 字段转为 Boolean
    val withPlatform = df
      .withColumn("win_support",
        when(upper($"Windows") === "TRUE" || $"Windows" === "1", true).otherwise(false))
      .withColumn("mac_support",
        when(upper($"Mac") === "TRUE" || $"Mac" === "1", true).otherwise(false))
      .withColumn("linux_support",
        when(upper($"Linux") === "TRUE" || $"Linux" === "1", true).otherwise(false))
      .withColumn("platform_count",
        $"win_support".cast("int") +
          $"mac_support".cast("int") +
          $"linux_support".cast("int"))
      .withColumn("platform_category",
        when($"platform_count" >= 3, "三平台")
          .when($"platform_count" === 2, "双平台")
          .when($"platform_count" === 1, "单平台(Windows)")
          .otherwise("其他"))

    withPlatform
      .groupBy("platform_category")
      .agg(
        count("*").as("game_count"),
        round(avg($"owners_numeric"), 0).as("avg_owners")
      )
      .orderBy($"game_count".desc)
  }

  /**
   * 开发商类型分类
   */
  private def developerClassification(df: DataFrame): DataFrame = {
    val devGameCounts = df
      .groupBy("Developers")
      .agg(
        count("*").as("game_count"),
        sum($"owners_numeric").as("total_owners"),
        round(avg($"Price"), 2).as("avg_price"),
        round(avg($"Metacritic_score"), 1).as("avg_metacritic")
      )

    val withCategory = devGameCounts
      .withColumn("developer_category",
        when($"game_count" === 1, "独立开发者")
          .when($"game_count" >= 2 && $"game_count" <= 10, "小型厂商")
          .when($"game_count" >= 11 && $"game_count" <= 50, "中型厂商")
          .otherwise("大型厂商"))

    withCategory
      .groupBy("developer_category")
      .agg(
        count("*").as("developer_count"),
        sum("game_count").as("total_games"),
        round(avg("avg_price"), 2).as("avg_game_price"),
        round(avg("avg_metacritic"), 1).as("avg_metacritic")
      )
      .orderBy($"developer_count".desc)
  }
}
