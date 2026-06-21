package com.spark.project.analysis.member1

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 分析功能3: 游戏类型与标签共现关联挖掘
 *
 * 分析目标:
 * - Genres（游戏类型）频率分布统计
 * - Tags（用户标签）频率分布与 Top-50 分析
 * - Genres-Tags 关联分析
 * - FP-Growth 频繁标签组合挖掘
 *
 * 使用字段: Genres, Tags, Categories, owners_numeric
 */
object Analysis3 {

  /**
   * 执行分析
   *
   * @param spark SparkSession
   * @param df    清洗后的 Steam 数据
   * @return 分析结果 DataFrame
   */
  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A3] 类型标签关联挖掘 - 开始...")

    // ===== Part 1: Genres 频率分布 =====
    println("[A3] Part 1: Genres 频率分布统计...")
    val genreDist = genreDistribution(df)
    println(s"[A3] Genres 类型数: ${genreDist.count()}")
    genreDist.show(10, truncate = false)

    // ===== Part 2: Tags 频率分布 =====
    println("[A3] Part 2: Tags 频率分布统计 (Top-50)...")
    val tagDist = tagDistribution(df)
    println(s"[A3] Tags 标签数: ${tagDist.count()}")
    tagDist.show(10, truncate = false)

    // ===== Part 3: 高销量类型分析 =====
    println("[A3] Part 3: 高销量类型分析...")
    val topSelling = topSellingGenres(df)
    topSelling.show(10, truncate = false)

    // ===== Part 4: FP-Growth 标签组合挖掘 =====
    println("[A3] Part 4: FP-Growth 频繁标签组合挖掘...")
    val freqCombos = frequentTagCombinations(spark, df)

    println(s"[A3] 类型标签挖掘 - 完成")

    // 返回综合结果：Top-50 标签 + 销量关联作为最终输出
    tagDist
      .join(topSelling,
        tagDist("tag") === topSelling("genre"),
        "left_outer")
      .select(
        $"tag",
        $"tag_count".as("frequency"),
        round($"avg_owners", 0).as("avg_owners"),
        $"total_games"
      )
      .orderBy($"frequency".desc)
      .limit(50)
  }

  /**
   * 解析数组字段为多行（explode）
   */
  private def parseAndExplode(df: DataFrame, colName: String, alias: String): DataFrame = {
    // 清洗 JSON 数组格式: "['Action','Indie']" → Action, Indie
    val cleaned = df
      .withColumn(s"${colName}_cleaned",
        regexp_replace(
          regexp_replace(col(colName), "^\\[|\\]$", ""),
          "['\"]", ""
        )
      )
      .withColumn(alias,
        explode(
          split(col(s"${colName}_cleaned"), ",")
        )
      )
      .withColumn(alias, trim(col(alias)))
      .filter(col(alias).isNotNull && col(alias) =!= "")

    cleaned
  }

  /**
   * 类型频率统计
   */
  private def genreDistribution(df: DataFrame): DataFrame = {
    val exploded = parseAndExplode(df, "Genres", "genre")

    exploded
      .groupBy("genre")
      .agg(
        count("*").as("count"),
        round(avg($"owners_numeric"), 0).as("avg_owners")
      )
      .orderBy($"count".desc)
  }

  /**
   * 标签频率统计
   */
  private def tagDistribution(df: DataFrame): DataFrame = {
    val exploded = parseAndExplode(df, "Tags", "tag")

    exploded
      .groupBy("tag")
      .agg(
        count("*").as("tag_count"),
        round(avg($"owners_numeric"), 0).as("avg_owners"),
        countDistinct("AppID").as("total_games")
      )
      .orderBy($"tag_count".desc)
      .limit(50)
  }

  /**
   * FP-Growth 频繁标签组合挖掘
   */
  private def frequentTagCombinations(spark: SparkSession, df: DataFrame): DataFrame = {
    // 为每个游戏构建标签数组
    val tagArrays = df
      .withColumn("Tags_cleaned",
        regexp_replace(
          regexp_replace($"Tags", "^\\[|\\]$", ""),
          "['\"]", ""
        )
      )
      .withColumn("tag_array",
        split(col("Tags_cleaned"), ",")
      )
      .withColumn("tag_array",
        expr("transform(tag_array, x -> trim(x))")
      )
      .filter(size($"tag_array") > 1) // 至少 2 个标签
      .select($"tag_array".as("items"))

    if (tagArrays.count() == 0) {
      println("[A3] FP-Growth: 无足够的标签数组数据")
      return spark.emptyDataFrame
    }

    // FP-Growth 模型
    val fpg = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.02)  // 最小支持度 2%
      .setMinConfidence(0.3) // 最小置信度 30%

    val model = fpg.fit(tagArrays)

    // 频繁项集
    println(s"[A3] 频繁标签组合 Top-20 (minSupport=0.02):")
    model.freqItemsets
      .orderBy($"freq".desc)
      .show(20, truncate = false)

    // 关联规则
    println(s"[A3] 关联规则 Top-20 (minConfidence=0.3):")
    model.associationRules
      .orderBy($"confidence".desc)
      .show(20, truncate = false)

    model.freqItemsets
  }

  /**
   * 高销量类型分析
   */
  private def topSellingGenres(df: DataFrame): DataFrame = {
    val exploded = parseAndExplode(df, "Genres", "genre")

    exploded
      .groupBy("genre")
      .agg(
        count("*").as("total_games"),
        round(avg($"owners_numeric"), 0).as("avg_owners"),
        round(sum($"owners_numeric"), 0).as("total_owners")
      )
      .orderBy($"avg_owners".desc)
  }
}
