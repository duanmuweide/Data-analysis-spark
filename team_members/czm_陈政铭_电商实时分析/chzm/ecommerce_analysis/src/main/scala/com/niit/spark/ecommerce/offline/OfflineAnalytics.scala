package com.niit.spark.ecommerce.offline

import com.niit.spark.ecommerce.common.{ProductCategory, TextValues}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object OfflineAnalytics {

  // A1: time-series statistics. Shows day/hour sales trend and peak periods.
  def salesTimeTrend(validOrders: DataFrame): DataFrame = {
    validOrders
      .groupBy(col("stat_date"), col("stat_hour"))
      .agg(
        countDistinct(col("order_id")).as("order_count"),
        countDistinct(col("user_id")).as("user_count"),
        round(sum(col("total_amount")), 2).as("total_amount"),
        round(sum(col("discount_amount")), 2).as("discount_amount"),
        round(sum(col("pay_amount")), 2).as("pay_amount"),
        round(avg(col("pay_amount")), 2).as("avg_order_amount")
      )
      .withColumn("update_time", current_timestamp())
      .orderBy(col("stat_date"), col("stat_hour"))
  }

  // A2: multidimensional aggregation plus daily TopN ranking by category.
  def categorySalesRank(validOrders: DataFrame, orderItems: DataFrame, products: DataFrame, topN: Int): DataFrame = {
    val validOrderKeys = validOrders.select(col("order_id"), col("stat_date"), col("channel"))
    val items = ProductCategory.attach(orderItems, products)

    val categoryDaily = items
      .join(validOrderKeys, Seq("order_id"), "inner")
      .groupBy(col("stat_date"), col("final_category").as("category"))
      .agg(
        countDistinct(col("order_id")).as("order_count"),
        sum(col("quantity")).cast(LongType).as("quantity_sum"),
        round(sum(col("amount")), 2).as("amount_sum"),
        round(avg(col("amount")), 2).as("avg_item_amount"),
        countDistinct(col("channel")).as("channel_count")
      )

    val rankWindow = Window.partitionBy(col("stat_date")).orderBy(col("amount_sum").desc, col("quantity_sum").desc)

    categoryDaily
      .withColumn("category_rank", row_number().over(rankWindow))
      .filter(col("category_rank") <= lit(topN))
      .withColumn("update_time", current_timestamp())
      .orderBy(col("stat_date"), col("category_rank"))
  }

  // A3: RFM user value segmentation. This is segmentation, not another groupBy count.
  def userValueLevel(validOrders: DataFrame, users: DataFrame): DataFrame = {
    val maxDate = validOrders
      .agg(date_format(max(col("stat_date")), "yyyy-MM-dd").as("max_date"))
      .first()
      .getAs[String]("max_date")

    if (maxDate == null) {
      throw new IllegalStateException("Cannot calculate RFM because valid orders have no stat_date")
    }

    val referenceDate: Column = date_add(to_date(lit(maxDate), "yyyy-MM-dd"), 1)

    val base = validOrders
      .groupBy(col("user_id"))
      .agg(
        datediff(referenceDate, max(col("stat_date"))).cast(IntegerType).as("recency_days"),
        countDistinct(col("order_id")).cast(LongType).as("frequency"),
        round(sum(col("pay_amount")), 2).as("monetary")
      )

    val rWindow = Window.orderBy(col("recency_days").desc)
    val fWindow = Window.orderBy(col("frequency").asc)
    val mWindow = Window.orderBy(col("monetary").asc)

    base
      .join(users.select("user_id", "province", "city", "member_level"), Seq("user_id"), "left")
      .withColumn("r_score", ntile(5).over(rWindow))
      .withColumn("f_score", ntile(5).over(fWindow))
      .withColumn("m_score", ntile(5).over(mWindow))
      .withColumn("total_score", col("r_score") + col("f_score") + col("m_score"))
      .withColumn(
        "user_level",
        when(col("total_score") >= 13, TextValues.HighValueUser)
          .when(col("total_score") >= 10, TextValues.PotentialUser)
          .when(col("total_score") >= 7, TextValues.NormalUser)
          .otherwise(TextValues.LowValueUser)
      )
      .withColumn("update_time", current_timestamp())
      .select(
        col("user_id"),
        col("province"),
        col("city"),
        col("member_level"),
        col("recency_days"),
        col("frequency"),
        col("monetary"),
        col("r_score"),
        col("f_score"),
        col("m_score"),
        col("total_score"),
        col("user_level"),
        col("update_time")
      )
  }

  // A4: category association rules. Uses co-occurrence support, confidence and lift.
  def categoryAssociationRules(
    validOrders: DataFrame,
    orderItems: DataFrame,
    products: DataFrame,
    minSupport: Double,
    minConfidence: Double
  ): DataFrame = {
    val validOrderIds = validOrders.select(col("order_id")).distinct()
    val items = ProductCategory.attach(orderItems, products)

    val baskets = items
      .join(validOrderIds, Seq("order_id"), "inner")
      .groupBy(col("order_id"))
      .agg(collect_set(col("final_category")).as("categories"))
      .filter(size(col("categories")) >= 2)
      .cache()

    val totalBasketCount = baskets.count()
    if (totalBasketCount == 0) {
      throw new IllegalStateException("No multi-category baskets found; cannot calculate association rules")
    }

    val categoryCounts = baskets
      .withColumn("category", explode(col("categories")))
      .groupBy(col("category"))
      .agg(countDistinct(col("order_id")).as("category_order_count"))

    val pairCounts = baskets
      .withColumn("antecedent", explode(col("categories")))
      .withColumn("consequent", explode(col("categories")))
      .filter(col("antecedent") =!= col("consequent"))
      .groupBy(col("antecedent"), col("consequent"))
      .agg(countDistinct(col("order_id")).as("pair_order_count"))

    val antecedentCounts = categoryCounts
      .select(col("category").as("antecedent"), col("category_order_count").as("antecedent_order_count"))

    val consequentCounts = categoryCounts
      .select(col("category").as("consequent"), col("category_order_count").as("consequent_order_count"))

    pairCounts
      .join(antecedentCounts, Seq("antecedent"), "inner")
      .join(consequentCounts, Seq("consequent"), "inner")
      .withColumn("total_basket_count", lit(totalBasketCount))
      .withColumn("support", round(col("pair_order_count").cast(DoubleType) / lit(totalBasketCount.toDouble), 6))
      .withColumn("confidence", round(col("pair_order_count").cast(DoubleType) / col("antecedent_order_count").cast(DoubleType), 6))
      .withColumn("lift", round(col("confidence") / (col("consequent_order_count").cast(DoubleType) / lit(totalBasketCount.toDouble)), 6))
      .filter(col("support") >= lit(minSupport) && col("confidence") >= lit(minConfidence))
      .withColumn("update_time", current_timestamp())
      .select(
        col("antecedent"),
        col("consequent"),
        col("support"),
        col("confidence"),
        col("lift"),
        col("pair_order_count"),
        col("antecedent_order_count"),
        col("consequent_order_count"),
        col("total_basket_count"),
        col("update_time")
      )
      .orderBy(col("lift").desc, col("confidence").desc, col("support").desc)
  }
}