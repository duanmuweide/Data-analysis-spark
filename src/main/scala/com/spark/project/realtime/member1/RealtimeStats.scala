package com.spark.project.realtime.member1

import com.spark.project.common.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import java.sql.Timestamp
import java.util.Properties

/**
 * 实时流统计分析（加分项）: Steam 游戏实时数据仪表盘
 *
 * 数据流: Kafka → Spark Structured Streaming → 窗口聚合 → MySQL
 * 前端: dashboard.jsp 每 5 秒轮询 /api/realtime/latest
 *
 * Kafka Topic: steam-game-events
 * 消息格式: JSON { app_id, name, price, genres, estimated_owners, positive, negative, timestamp }
 *
 * 输出表:
 *   - realtime_game_stats  : 最新窗口聚合指标（新游戏数、均价、好评率等）
 *   - realtime_genre_counts: 最新窗口内各类型分布
 */
object RealtimeStats {

  // JSON 消息 Schema
  val EVENT_SCHEMA: StructType = StructType(Array(
    StructField("app_id", LongType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("genres", StringType, nullable = true),      // JSON array string
    StructField("estimated_owners", LongType, nullable = true),
    StructField("positive", IntegerType, nullable = true),
    StructField("negative", IntegerType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true)
  ))

  /** 统计输出表的窗口列顺序 (window_start, window_end, ...) */
  private val WINDOW_COLS: Seq[String] =
    Seq("window_start", "window_end", "new_games_count",
        "avg_price", "avg_owners", "avg_positive_rate")

  /** 类型输出表的列顺序 */
  private val GENRE_COLS: Seq[String] =
    Seq("genre", "count", "window_start", "window_end")

  // ===================================================================
  // 启动实时流
  // ===================================================================

  /**
   * 启动实时统计任务
   *
   * @param spark SparkSession
   * @return StreamingQuery 句柄
   */
  def start(spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    println("[Realtime] 启动实时流处理...")

    // 1. 从 Kafka 读取流数据
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", Config.KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "500")
      .option("failOnDataLoss", "false")
      .load()

    // 2. 解析 JSON 消息
    val parsedStream = kafkaStream
      .select(
        from_json(
          $"value".cast(StringType),
          EVENT_SCHEMA
        ).as("data")
      )
      .select("data.*")
      .filter($"app_id".isNotNull)

    // 3. 手动累计（每批500条，直接累加写 MySQL）
    var total: Long = 0; var priceSum: Double = 0; var ownersSum: Double = 0
    var posSum: Long = 0; var negSum: Long = 0

    val query = parsedStream.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"${Config.DATA_ROOT}/checkpoints/realtime_stats")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        val cnt = batchDF.count()
        if (cnt > 0) {
          val row = batchDF.agg(
            count("*"), avg("price"), avg("estimated_owners"), avg("positive"), avg("negative")
          ).collect()(0)
          val c = row.getLong(0)
          total += c
          priceSum += row.getDouble(1) * c
          ownersSum += (if (row.isNullAt(2)) 0 else row.getDouble(2)) * c
          posSum += (if (row.isNullAt(3)) 0L else row.getDouble(3).toLong)
          negSum += (if (row.isNullAt(4)) 0L else row.getDouble(4).toLong)
          val avgP = f"${priceSum / total}%.2f".toDouble
          val avgO = (ownersSum / total).toLong
          val avgR = f"${posSum.toDouble / (posSum + negSum + 1) * 100}%.1f".toDouble

          val conn = java.sql.DriverManager.getConnection(Config.JDBC_URL, Config.DB_USER, Config.DB_PASSWORD)
          try {
            conn.createStatement().execute("TRUNCATE TABLE realtime_game_stats")
            val ps = conn.prepareStatement("INSERT INTO realtime_game_stats VALUES (NOW(),NOW(),?,?,?,?,NOW())")
            ps.setLong(1, total); ps.setDouble(2, avgP); ps.setDouble(3, avgO); ps.setDouble(4, avgR)
            ps.executeUpdate(); ps.close()
          } finally { conn.close() }
          println(s"[Realtime] ✅ $total 条 | 均价:$avgP | 好评率:$avgR%")
        }
      }
      .start()

    println("[Realtime] 手动累计已启动 → MySQL::realtime_game_stats")

    query
  }

  /** 使用 TRUNCATE + INSERT 避免 DROP TABLE 导致查询空窗 */
  private def truncateAndWrite(df: DataFrame, table: String, props: Properties): Unit = {
    val conn = java.sql.DriverManager.getConnection(Config.JDBC_URL, Config.DB_USER, Config.DB_PASSWORD)
    try {
      conn.createStatement().execute(s"TRUNCATE TABLE $table")
      conn.close()
    } catch {
      case _: Exception => // 表不存在则建表
    }
    df.write.mode("append").jdbc(Config.JDBC_URL, table, props)
  }

  /** 创建 JDBC 连接属性 */
  private def jdbcProps(): Properties = {
    val props = new Properties()
    props.setProperty("user", Config.DB_USER)
    props.setProperty("password", Config.DB_PASSWORD)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    props
  }

  // ===================================================================
  // 查询辅助方法（供 Web API 通过 Spark 内存表调用 — 兼容旧方案）
  // ===================================================================

  /**
   * 获取最新实时统计（供 Web API 调用 — 优先用 MySQL 方案）
   */
  def getLatestStats(spark: SparkSession): DataFrame = {
    try {
      spark.sql(
        """SELECT *
          |FROM realtime_game_stats
          |ORDER BY window_end DESC
          |LIMIT 1""".stripMargin)
    } catch {
      case _: Exception =>
        spark.emptyDataFrame
    }
  }

  /**
   * 获取最近 N 个窗口的统计
   */
  def getRecentStats(spark: SparkSession, n: Int = 10): DataFrame = {
    try {
      spark.sql(
        s"""SELECT *
           |FROM realtime_game_stats
           |ORDER BY window_end DESC
           |LIMIT $n""".stripMargin)
    } catch {
      case _: Exception =>
        spark.emptyDataFrame
    }
  }
}
