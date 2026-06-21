package com.spark.project.realtime.member1

import com.spark.project.common.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
 * 实时流统计分析（加分项）: Steam 游戏实时数据仪表盘
 *
 * 数据流: Kafka → Spark Structured Streaming → 窗口聚合 → 内存表
 * 前端: dashboard.jsp 每 5 秒轮询 /api/realtime/latest
 *
 * Kafka Topic: steam-game-events
 * 消息格式: JSON { app_id, name, price, genres, estimated_owners, positive, negative, timestamp }
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

    // 3. 添加窗口列和水印
    val withWatermark = parsedStream
      .withWatermark("timestamp", "1 minute")
      .withColumn("genres_array",
        when($"genres".isNotNull,
          split(
            regexp_replace(regexp_replace($"genres", "^\\[|\\]$", ""), "['\"]", ""),
            ","
          )
        )
      )

    // 4. 滑动窗口聚合（窗口60秒，滑动10秒）
    val windowedAgg = withWatermark
      .groupBy(
        window($"timestamp", "60 seconds", "10 seconds")
      )
      .agg(
        count("*").as("new_games_count"),
        round(avg($"price"), 2).as("avg_price"),
        round(avg($"estimated_owners"), 0).as("avg_owners"),
        round(avg($"positive") / (avg($"positive") + avg($"negative") + 1) * 100, 1)
          .as("avg_positive_rate")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"new_games_count",
        $"avg_price",
        $"avg_owners",
        $"avg_positive_rate"
      )

    // 5. 输出到内存表
    val query = windowedAgg.writeStream
      .outputMode(OutputMode.Append())
      .format("memory")
      .queryName("realtime_game_stats")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation",
        s"${Config.DATA_ROOT}/checkpoints/realtime_stats")
      .start()

    println("[Realtime] 实时流处理已启动 - 查询名: realtime_game_stats")
    println(s"[Realtime] Kafka: ${Config.KAFKA_BOOTSTRAP_SERVERS}, Topic: ${Config.KAFKA_TOPIC}")

    query
  }

  /**
   * 获取最新实时统计（供 Web API 调用）
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
