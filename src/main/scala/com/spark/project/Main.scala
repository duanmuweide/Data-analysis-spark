package com.spark.project

import com.spark.project.common.{Config, SparkSessionBuilder, Utils}
import com.spark.project.analysis.member1.{Analysis1 => A1, Analysis2 => A2, Analysis3 => A3, Analysis4 => A4}
import com.spark.project.realtime.member1.RealtimeStats
import org.apache.spark.sql.SparkSession

/**
 * Steam 游戏数据分析平台 — 主入口
 *
 * 启动流程:
 * 1. 创建 SparkSession（根据提交参数自动识别集群/本地模式）
 * 2. 读取 Steam 游戏数据
 * 3. 依次执行 4 个离线分析任务
 * 4. 结果写入 MySQL，供 Web 仪表盘展示
 * 5. （可选）启动实时流处理 (Kafka → Streaming → MySQL)
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
 *
 * 本地开发 + 启动实时流:
 *   spark-submit --class com.spark.project.Main \
 *                --master local[*] \
 *                target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar \
 *                --sample --streaming
 */
object Main {

  def main(args: Array[String]): Unit = {

    val useSampleData = args.contains("--sample")
    val enableStreaming = args.contains("--streaming")
    val streamingOnly = args.contains("--streaming-only")

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
      var cleanedDF: org.apache.spark.sql.DataFrame = null

      if (!streamingOnly) {
      // ============================
      // Step 2: 读取 + 清洗数据
      // ============================
      println("=" * 70)
      println("[Main] Step 1: 读取并清洗 Steam 游戏数据...")
      println("=" * 70)

      val rawDF = Utils.readSteamData(spark, dataPath, useSampleData)
      println(s"[Main] 原始数据: ${rawDF.count()} 行, ${rawDF.columns.length} 列")

      cleanedDF = Utils.cleanSteamData(rawDF)
      println(s"[Main] 清洗后数据: ${cleanedDF.count()} 行")

      // 注意：不缓存数据以免 OOM（内存仅 366MB 时 cached 390MB CSV 会爆内存）
      // 115k 行数据量不大，重复扫描开销远低于 OOM 风险
      println(s"[Main] 数据就绪: ${cleanedDF.count()} 行（未缓存，避免 OOM）")
      }

      // ============================
      // Step 3: 离线批处理分析（--streaming-only 时跳过）
      // ============================
      if (!streamingOnly) {
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
      // Step 4: 离线分析完成
      // ============================
      println("\n" + "=" * 70)
      println("[Main] 全部 4 个离线分析已完成！")
      println(s"[Main] 结果写入 MySQL: ${Config.JDBC_URL}")
      println("[Main] 启动 Web 服务器查看仪表盘: http://localhost:8080")
      println("=" * 70)
      } // end if (!streamingOnly)

      // ============================
      // Step 5: 启动实时流处理（可选）
      // ============================
      if (enableStreaming || streamingOnly) {
        println("\n" + "=" * 70)
        println("[Main] Step 3: 启动实时流处理...")
        println("=" * 70)

        try {
          val streamingQuery = RealtimeStats.start(spark)
          println("[Main] ✅ 实时流处理已启动！")
          println("[Main] Kafka → Spark Streaming → MySQL (realtime_game_stats / realtime_genre_counts)")
          println("[Main] 前端每 5 秒自动刷新实时数据")
          println("[Main] 按 Ctrl+C 停止...")
          println("=" * 70)

          // 保持应用运行，等待 Streaming 终止
          spark.streams.awaitAnyTermination()

        } catch {
          case e: Exception =>
            println(s"[Main] ⚠ 实时流启动失败: ${e.getMessage}")
            println("[Main] ⚠ 请检查 Kafka 集群是否运行")
            println("[Main] ⚠ 离线分析结果已写入 MySQL，Web 仪表盘仍可访问")
        }
      } else {
        println("[Main] 💡 提示: 添加 --streaming 参数可同时启动实时流处理")
        println("[Main]    示例: spark-submit ... --sample --streaming")
      }

      // 没有缓存，无需释放

    } catch {
      case e: Exception =>
        println(s"[Main] 执行出错: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      // 如果启动了 Streaming，finally 块会在 awaitAnyTermination 返回后执行
      spark.stop()
      println("[Main] SparkSession 已关闭")
    }
  }
}
