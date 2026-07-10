package com.spark.project

import com.spark.project.common.{Config, SparkSessionBuilder, Utils}
import com.spark.project.analysis.member1.{Analysis1 => A1, Analysis2 => A2, Analysis3 => A3, Analysis4 => A4}
import com.spark.project.realtime.member1.RealtimeStats
import com.spark.project.web.WebServer
import org.apache.spark.sql.SparkSession

/**
 * Steam 游戏数据分析平台 — 主入口
 *
 * 启动流程:
 * 1. 创建 SparkSession（根据提交参数自动识别集群/本地模式）
 * 2. 依次执行 4 个离线分析任务：
 *    - A1: Steam游戏市场趋势分析
 *    - A2: 游戏定价与用户评价关联分析
 *    - A3: 游戏类型与标签共现关联挖掘
 *    - A4: 开发商与发行商生态分析
 * 3. 启动实时流处理（加分项）
 * 4. 启动 Web 服务器，提供动态仪表盘
 *
 * 提交到 Spark 集群:
 *   spark-submit --class com.spark.project.Main \
 *                --master yarn \
 *                --deploy-mode cluster \
 *                --executor-memory 4G \
 *                --num-executors 3 \
 *                target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar
 *
 * 本地开发:
 *   spark-submit --class com.spark.project.Main \
 *                --master local[*] \
 *                --conf spark.sql.adaptive.enabled=true \
 *                target/scala-2.12/Sem6SparkProject-assembly-1.0.0.jar
 *
 * TODO: 实现完整的启动流程
 */
object Main {

  def main(args: Array[String]): Unit = {

    // ===================================================================
    // 步骤1: 创建 SparkSession
    // ===================================================================
    val useSampleData = args.contains("--sample")

    val spark: SparkSession = SparkSessionBuilder.createSession(
      "Steam-Games-Analysis"
    )
    // TODO: 实现 SparkSession 创建并打印配置信息

    try {
      // ===================================================================
      // 步骤2: 离线批处理分析 (4个分析模块)
      // ===================================================================

      println("=" * 70)
      println(">>> [分析1] Steam 游戏市场趋势分析")
      println("=" * 70)
      // val (result1, time1) = Utils.time { A1.run(spark) }
      // println(s">>> 完成! 耗时: ${time1}ms")
      // TODO: 保存结果到 MySQL / HDFS
      // Utils.saveToDatabase(result1, "analysis_market_trend")

      println("=" * 70)
      println(">>> [分析2] 游戏定价与用户评价关联分析")
      println("=" * 70)
      // val (result2, time2) = Utils.time { A2.run(spark) }
      // println(s">>> 完成! 耗时: ${time2}ms")
      // TODO: 保存结果

      println("=" * 70)
      println(">>> [分析3] 游戏类型与标签共现关联挖掘")
      println("=" * 70)
      // val (result3, time3) = Utils.time { A3.run(spark) }
      // println(s">>> 完成! 耗时: ${time3}ms")
      // TODO: 保存结果

      println("=" * 70)
      println(">>> [分析4] 开发商与发行商生态分析")
      println("=" * 70)
      // val (result4, time4) = Utils.time { A4.run(spark) }
      // println(s">>> 完成! 耗时: ${time4}ms")
      // TODO: 保存结果

      println("=" * 70)
      println(">>> 全部离线分析完成!")
      println("=" * 70)

      // ===================================================================
      // 步骤3: 实时流处理（加分项）
      // ===================================================================
      println(">>> 启动实时流处理（加分项）...")
      // val realtimeQuery = RealtimeStats.start(spark)
      // TODO: 流处理异常处理 + 优雅关闭

      // ===================================================================
      // 步骤4: 启动 Web 服务器
      // ===================================================================
      println("=" * 70)
      println(s">>> 启动 Web 仪表盘: http://${Config.WEB_HOST}:${Config.WEB_PORT}")
      println(">>> 浏览器访问即可查看分析结果")
      println("=" * 70)

      // WebServer.start()
      // TODO: 保持进程运行，等待 Ctrl+C 优雅关闭

    } finally {
      // TODO: 优雅关闭
      // spark.stop()
    }
  }
}
