package com.spark.project.common

import org.apache.spark.sql.SparkSession

/**
 * SparkSession 构建器
 *
 * 统一管理 SparkSession 的创建和配置，确保：
 * - 集群模式下的资源分配正确
 * - 序列化、压缩等关键参数已配置
 * - 支持本地调试和集群部署两种模式
 *
 * TODO: 根据集群实际资源配置 executor 内存和核心数
 */
object SparkSessionBuilder {

  /**
   * 创建 SparkSession
   *
   * @param appName    应用名称，用于在YARN/SparkUI中标识
   * @param master     Master地址，集群提交时从 spark-submit 参数获取
   * @return 配置好的 SparkSession 实例
   */
  def createSession(appName: String, master: String = ""): SparkSession = {
    // TODO: 实现 SparkSession 创建逻辑
    // 1. 创建 SparkConf，配置基本参数
    // 2. 设置序列化器为 KryoSerializer
    // 3. 启用自适应查询执行 (AQE)
    // 4. 配置动态资源分配（如果集群支持）
    // 5. 创建并返回 SparkSession
    ???
  }

  /**
   * 本地调试用 SparkSession
   *
   * @param appName 应用名称
   * @return 本地模式的 SparkSession（仅开发调试用）
   */
  def createLocalSession(appName: String): SparkSession = {
    // TODO: 实现本地模式的 SparkSession
    // 设置 master 为 local[*]
    ???
  }
}
