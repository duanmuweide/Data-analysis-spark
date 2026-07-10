package com.spark.project.analysis.member1

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 分析功能3: 游戏类型与标签共现关联挖掘
 */
object Analysis3 {

  def run(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    println("[A3] 类型标签关联挖掘 - 开始...")

    val genreDist = genreDistribution(spark, df)
    println(s"[A3] Genres 类型数: ${genreDist.count()}")
    genreDist.show(10)

    val tagDist = tagDistribution(spark, df)
    println(s"[A3] Tags 标签数: ${tagDist.count()}")
    tagDist.show(10)

    val topSelling = topSellingGenres(spark, df)
    topSelling.show(10)

    freqTagCombinations(spark, df)

    println(s"[A3] 完成")

    tagDist
      .select($"tag", $"tag_count".as("frequency"), $"avg_owners", $"total_games")
      .orderBy($"frequency".desc)
      .limit(50)
  }

  private def genreDistribution(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val exploded = parseAndExplode(spark, df, "Genres", "genre")
    exploded.groupBy("genre").agg(
      count("*").as("count"),
      round(avg($"owners_numeric"), 0).as("avg_owners")
    ).orderBy($"count".desc)
  }

  private def tagDistribution(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val exploded = parseAndExplode(spark, df, "Tags", "tag")
    exploded.groupBy("tag").agg(
      count("*").as("tag_count"),
      round(avg($"owners_numeric"), 0).as("avg_owners"),
      countDistinct("AppID").as("total_games")
    ).orderBy($"tag_count".desc).limit(50)
  }

  private def freqTagCombinations(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    val tagArrays = df
      .withColumn("tag_array", split(
        regexp_replace(regexp_replace($"Tags", "^\\[|\\]$", ""), "['\"]", ""),
        ","
      ))
      .withColumn("tag_array", expr("transform(tag_array, x -> trim(x))"))
      .filter(size($"tag_array") > 1)
      .select($"tag_array".as("items"))

    if (tagArrays.count() == 0) {
      println("[A3] FP-Growth: 无足够数据")
      return
    }

    val fpg = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.02)
      .setMinConfidence(0.3)

    val model = fpg.fit(tagArrays)

    println("[A3] 频繁标签组合 Top-20:")
    model.freqItemsets.orderBy($"freq".desc).show(20)

    println("[A3] 关联规则 Top-20:")
    model.associationRules.orderBy($"confidence".desc).show(20)
  }

  private def topSellingGenres(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val exploded = parseAndExplode(spark, df, "Genres", "genre")
    exploded.groupBy("genre").agg(
      count("*").as("total_games"),
      round(avg($"owners_numeric"), 0).as("avg_owners"),
      round(sum($"owners_numeric"), 0).as("total_owners")
    ).orderBy($"avg_owners".desc)
  }

  private def parseAndExplode(spark: SparkSession, df: DataFrame, colName: String, alias: String): DataFrame = {
    import spark.implicits._
    df
      .withColumn(s"${colName}_cleaned",
        regexp_replace(regexp_replace(col(colName), "^\\[|\\]$", ""), "['\"]", ""))
      .withColumn(alias, explode(split(col(s"${colName}_cleaned"), ",")))
      .withColumn(alias, trim(col(alias)))
      .filter(col(alias).isNotNull && col(alias) =!= "")
  }
}
