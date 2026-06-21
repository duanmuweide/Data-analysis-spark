package com.spark.project

import com.spark.project.common.{Config, SparkSessionBuilder, Utils}
import com.spark.project.analysis.member1.{Analysis1 => A1, Analysis2 => A2, Analysis3 => A3, Analysis4 => A4}
import org.apache.spark.sql.SparkSession

/**
 * Steam 游戏数据分析平台 — 主入口
 *
 * 启动流程:
 * 1. 创建 SparkSession（根据提交参数自动识别集群/本地模式）
 * 2. 读取 Steam 游戏数据
 * 3. 依次执行 4 个离线分析任务
 * 4. 结果写入 MySQL，供 Web 仪表盘展示
 *
 * 集群提交:
 *   spark-submit --class com.spark.project.Main \
 *                --master yarn \
 *                --deploy-mode cluster \
 *                --executor-memory 4G \
 *                --num-executors 3 \
 *                target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar
 *
 * 本地开发（使用样例数据）:
 *   spark-submit --class com.spark.project.Main \
 *                --master local[*] \
 *                target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar \
 *                --sample
 */
object Main {

  def main(args: Array[String]): Unit = {

    val useSampleData = args.contains("--sample")
    val dataPath = if (useSampleData) {
      println("[Main] 使用样例数据运行（14行，开发调试用）")
      Config.STEAM_SAMPLE_FILE
    } else {
      println("[Main] 使用完整数据运行（390MB，115k+ 行）")
      Config.STEAM_DATA_FILE
    }

    // ============================
    // Step 1: 创建 SparkSession
    // ============================
    val spark: SparkSession = SparkSessionBuilder.createSession("Steam-Games-Analysis")

    println("=" * 70)
    println(s"[Main] SparkSession 创建成功")
    println(s"[Main] Spark 版本: ${spark.version}")
    println(s"[Main] Master: ${spark.sparkContext.master}")
    println("=" * 70)

    try {
      // ============================
      // Step 2: 读取 + 清洗数据
      // ============================
      println("=" * 70)
      println("[Main] Step 1: 读取并清洗 Steam 游戏数据...")
      println("=" * 70)

      val rawDF = Utils.readSteamData(spark, dataPath, useSampleData)
      println(s"[Main] 原始数据: ${rawDF.count()} 行, ${rawDF.columns.length} 列")

      val cleanedDF = Utils.cleanSteamData(rawDF)
      println(s"[Main] 清洗后数据: ${cleanedDF.count()} 行")

      // 缓存清洗后的数据（多次分析复用）
      cleanedDF.cache()
      val cachedCount = cleanedDF.count() // 触发缓存
      println(s"[Main] 数据已缓存: $cachedCount 行")

      // ============================
      // Step 3: 离线批处理分析
      // ============================
      println("\n" + "=" * 70)
      println("[Main] Step 2: 开始执行 4 个离线分析任务")
      println("=" * 70)

      // --- Analysis 1: 市场趋势分析 ---
      println("\n>>> [A1] Steam 游戏市场趋势分析")
      println("-" * 70)
      val (result1, time1) = Utils.time { A1.run(spark, cleanedDF) }
      println(s">>> A1 完成! 结果: ${result1.count()} 行, 耗时: ${time1}ms")
      Utils.saveToDatabase(result1, "analysis_market_trend")

      // --- Analysis 2: 定价与评价关联分析 ---
      println("\n>>> [A2] 游戏定价与用户评价关联分析")
      println("-" * 70)
      val (result2, time2) = Utils.time { A2.run(spark, cleanedDF) }
      println(s">>> A2 完成! 结果: ${result2.count()} 行, 耗时: ${time2}ms")
      Utils.saveToDatabase(result2, "analysis_pricing_review")

      // --- Analysis 3: 类型标签关联挖掘 ---
      println("\n>>> [A3] 游戏类型与标签共现关联挖掘")
      println("-" * 70)
      val (result3, time3) = Utils.time { A3.run(spark, cleanedDF) }
      println(s">>> A3 完成! 结果: ${result3.count()} 行, 耗时: ${time3}ms")
      Utils.saveToDatabase(result3, "analysis_genre_tags")

      // --- Analysis 4: 开发商生态分析 ---
      println("\n>>> [A4] 开发商与发行商生态分析")
      println("-" * 70)
      val (result4, time4) = Utils.time { A4.run(spark, cleanedDF) }
      println(s">>> A4 完成! 结果: ${result4.count()} 行, 耗时: ${time4}ms")
      Utils.saveToDatabase(result4, "analysis_developer_ecosystem")

      // ============================
      // Step 4: 完成
      // ============================
      println("\n" + "=" * 70)
      println("[Main] 全部 4 个离线分析已完成！")
      println(s"[Main] 结果写入 MySQL: ${Config.JDBC_URL}")
      println("[Main] 启动 Web 服务器查看仪表盘: http://localhost:8080")
      println("=" * 70)

      // 释放缓存
      cleanedDF.unpersist()

    } catch {
      case e: Exception =>
        println(s"[Main] 执行出错: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
      println("[Main] SparkSession 已关闭")
    }
  }
}
