package com.niit.spark.ecommerce

import com.niit.spark.ecommerce.common.{EcommerceData, JobConfig, MysqlSink, TextValues}
import com.niit.spark.ecommerce.offline.OfflineAnalytics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OfflineJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.parse(args)
    if (config.showHelp) {
      println(JobConfig.usage)
      return
    }

    val spark = SparkSession.builder()
      .appName("czm-ecommerce-offline-analysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel(config.logLevel)
    spark.conf.set("spark.sql.shuffle.partitions", config.shufflePartitions.toString)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

    try {
      println("Input root: " + config.inputRoot)
      println("JDBC URL: " + config.jdbcUrl)
      println("Dry run: " + config.dryRun)

      val tables = EcommerceData.readAll(spark, config.inputRoot)
      tables.validate()

      val validOrders = tables.orders
        .filter(TextValues.validOrderStatus())
        .cache()

      val validOrderCount = validOrders.count()
      if (validOrderCount == 0) {
        throw new IllegalStateException("No paid or completed orders found in orders.csv")
      }
      println("Valid paid/completed orders: " + validOrderCount)

      val salesTimeTrend = OfflineAnalytics.salesTimeTrend(validOrders)
      val categorySalesRank = OfflineAnalytics.categorySalesRank(validOrders, tables.orderItems, tables.products, config.topN)
      val userValueLevel = OfflineAnalytics.userValueLevel(validOrders, tables.users)
      val associationRules = OfflineAnalytics.categoryAssociationRules(
        validOrders,
        tables.orderItems,
        tables.products,
        config.minSupport,
        config.minConfidence
      )

      MysqlSink.writeFullRefresh("ads_sales_time_trend", salesTimeTrend, config)
      MysqlSink.writeFullRefresh("ads_category_sales_rank", categorySalesRank, config)
      MysqlSink.writeFullRefresh("ads_user_value_level", userValueLevel, config)
      MysqlSink.writeFullRefresh("ads_category_association_rules", associationRules, config)

      println("Offline analysis finished.")
    } finally {
      spark.stop()
    }
  }
}