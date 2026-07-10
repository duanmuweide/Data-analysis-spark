package com.niit.spark.ecommerce

import com.niit.spark.ecommerce.common.{JobConfig, MysqlSink, Schemas, TextValues}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.parse(args)
    if (config.showHelp) {
      println(JobConfig.usage)
      return
    }

    val spark = SparkSession.builder()
      .appName("czm-ecommerce-streaming-analysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel(config.logLevel)
    spark.conf.set("spark.sql.shuffle.partitions", config.shufflePartitions.toString)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

    println("Kafka bootstrap: " + config.kafkaBootstrap)
    println("Kafka topic: " + config.kafkaTopic)
    println("Checkpoint: " + config.checkpointLocation)

    val kafkaSource = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrap)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    val events = kafkaSource
      .selectExpr("CAST(value AS STRING) AS json_text")
      .select(from_json(col("json_text"), Schemas.orderEvent).as("event"))
      .select("event.*")
      .withColumn("event_ts", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("pay_amount", coalesce(col("pay_amount"), lit(0.0)))
      .filter(col("order_id").isNotNull && col("event_ts").isNotNull && col("category").isNotNull)
      .filter(TextValues.validOrderStatus())

    val categoryWindowSales = events
      .withWatermark("event_ts", "2 minutes")
      .groupBy(window(col("event_ts"), config.windowDuration, config.slideDuration), col("category"))
      .agg(
        count(lit(1)).as("order_count"),
        round(sum(col("pay_amount")), 2).as("pay_amount")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("category"),
        col("order_count"),
        col("pay_amount")
      )

    val query = categoryWindowSales.writeStream
      .outputMode("complete")
      .option("checkpointLocation", config.checkpointLocation)
      .trigger(Trigger.ProcessingTime(config.triggerSeconds + " seconds"))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val output = batch
          .withColumn("batch_id", lit(batchId))
          .withColumn("update_time", current_timestamp())
        MysqlSink.writeFullRefresh("rt_category_window_sales", output, config.copy(dryRun = false))
      }
      .start()

    query.awaitTermination()
  }
}