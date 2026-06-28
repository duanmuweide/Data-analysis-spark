package com.spark.project.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * SparkSession 构建器
 *
 * 统一管理 SparkSession 的创建和配置，确保：
 * - 集群模式下的资源分配正确
 * - 序列化、压缩等关键参数已配置
 * - 支持本地调试和集群部署两种模式
 */
object SparkSessionBuilder {

  /**
   * 创建 SparkSession（自动识别本地/集群模式）
   *
   * @param appName 应用名称，用于在YARN/SparkUI中标识
   * @param master  Master地址，集群提交时从 spark-submit 参数获取
   * @return 配置好的 SparkSession 实例
   */
  def createSession(appName: String, master: String = ""): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)

    // 如果传入了 master，使用传入值；否则由 spark-submit 决定
    if (master.nonEmpty) {
      conf.setMaster(master)
    }

    // 核心配置
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.adaptive.enabled", Config.ADAPTIVE_QUERY_ENABLED.toString)
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS.toString)

    // 本地模式特殊配置
    if (master.contains("local")) {
      conf.set("spark.sql.shuffle.partitions", "8") // 本地模式：多分区降低 OOM 风险
      conf.set("spark.driver.memory", "2g") // 请求 2G 驱动内存
    }

    // 压缩优化
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
    conf.set("spark.sql.files.maxPartitionBytes", "134217728") // 128MB per partition

    SparkSession.builder()
      .config(conf)
      .enableHiveSupport() // 启用 Hive 支持（用于 SQL 函数）
      .getOrCreate()
  }

  /**
   * 本地调试用 SparkSession
   *
   * @param appName 应用名称
   * @return 本地模式的 SparkSession（仅开发调试用）
   */
  def createLocalSession(appName: String): SparkSession = {
    createSession(appName, "local[*]")
  }
}
