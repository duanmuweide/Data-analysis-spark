package com.niit.spark.ecommerce.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

final case class JobConfig(
  inputRoot: String = "/user/czm/ecommerce/raw",
  jdbcUrl: String = sys.env.getOrElse(
    "MYSQL_JDBC_URL",
    "jdbc:mysql://master-pc:3306/spark_ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
  ),
  jdbcUser: String = sys.env.getOrElse("MYSQL_USER", "spark"),
  jdbcPassword: String = sys.env.getOrElse("MYSQL_PASSWORD", "spark123456"),
  shufflePartitions: Int = 6,
  writePartitions: Int = 1,
  topN: Int = 10,
  minSupport: Double = 0.005,
  minConfidence: Double = 0.10,
  kafkaBootstrap: String = "master-pc:9092",
  kafkaTopic: String = "czm_order_events",
  checkpointLocation: String = "/user/czm/ecommerce/checkpoint/rt_category_window_sales",
  windowDuration: String = "1 minute",
  slideDuration: String = "30 seconds",
  triggerSeconds: Int = 10,
  dryRun: Boolean = false,
  logLevel: String = "WARN",
  showHelp: Boolean = false
)

object JobConfig {
  val usage: String =
    """
      |Common options:
      |  --jdbc-url <url>
      |  --jdbc-user <user>
      |  --jdbc-password <password>
      |  --shuffle-partitions <number>
      |  --write-partitions <number>
      |  --dry-run
      |  --log-level <level>
      |  --help
      |
      |Offline options:
      |  --input <path>
      |  --top-n <number>
      |  --min-support <number>
      |  --min-confidence <number>
      |
      |Streaming options:
      |  --kafka-bootstrap <host:port>
      |  --topic <topic>
      |  --checkpoint <hdfs-path>
      |  --window-duration <duration>
      |  --slide-duration <duration>
      |  --trigger-seconds <number>
      |""".stripMargin

  def parse(args: Array[String]): JobConfig = {
    var config = JobConfig()
    var i = 0

    def nextValue(flag: String): String = {
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("Missing value for " + flag)
      }
      i += 1
      args(i)
    }

    while (i < args.length) {
      args(i) match {
        case "--input" => config = config.copy(inputRoot = nextValue("--input"))
        case "--jdbc-url" => config = config.copy(jdbcUrl = nextValue("--jdbc-url"))
        case "--jdbc-user" => config = config.copy(jdbcUser = nextValue("--jdbc-user"))
        case "--jdbc-password" => config = config.copy(jdbcPassword = nextValue("--jdbc-password"))
        case "--shuffle-partitions" => config = config.copy(shufflePartitions = nextValue("--shuffle-partitions").toInt)
        case "--write-partitions" => config = config.copy(writePartitions = nextValue("--write-partitions").toInt)
        case "--top-n" => config = config.copy(topN = nextValue("--top-n").toInt)
        case "--min-support" => config = config.copy(minSupport = nextValue("--min-support").toDouble)
        case "--min-confidence" => config = config.copy(minConfidence = nextValue("--min-confidence").toDouble)
        case "--kafka-bootstrap" => config = config.copy(kafkaBootstrap = nextValue("--kafka-bootstrap"))
        case "--topic" => config = config.copy(kafkaTopic = nextValue("--topic"))
        case "--checkpoint" => config = config.copy(checkpointLocation = nextValue("--checkpoint"))
        case "--window-duration" => config = config.copy(windowDuration = nextValue("--window-duration"))
        case "--slide-duration" => config = config.copy(slideDuration = nextValue("--slide-duration"))
        case "--trigger-seconds" => config = config.copy(triggerSeconds = nextValue("--trigger-seconds").toInt)
        case "--dry-run" => config = config.copy(dryRun = true)
        case "--log-level" => config = config.copy(logLevel = nextValue("--log-level"))
        case "--help" | "-h" => config = config.copy(showHelp = true)
        case other => throw new IllegalArgumentException("Unknown argument: " + other + "\n" + usage)
      }
      i += 1
    }

    require(config.shufflePartitions > 0, "--shuffle-partitions must be positive")
    require(config.writePartitions > 0, "--write-partitions must be positive")
    require(config.topN > 0, "--top-n must be positive")
    require(config.minSupport >= 0.0 && config.minSupport <= 1.0, "--min-support must be between 0 and 1")
    require(config.minConfidence >= 0.0 && config.minConfidence <= 1.0, "--min-confidence must be between 0 and 1")
    require(config.triggerSeconds > 0, "--trigger-seconds must be positive")
    config
  }
}

object TextValues {
  val Paid: String = "\u5df2\u652f\u4ed8"
  val Completed: String = "\u5df2\u5b8c\u6210"
  val HighValueUser: String = "\u9ad8\u4ef7\u503c\u7528\u6237"
  val PotentialUser: String = "\u91cd\u70b9\u53d1\u5c55\u7528\u6237"
  val NormalUser: String = "\u4e00\u822c\u4fdd\u6301\u7528\u6237"
  val LowValueUser: String = "\u4f4e\u4ef7\u503c\u7528\u6237"

  def validOrderStatus(columnName: String = "order_status"): Column = {
    col(columnName).isin(Paid, Completed)
  }
}

object Schemas {
  val users: StructType = StructType(Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("user_name", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("province", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("member_level", StringType, nullable = true),
    StructField("register_date", StringType, nullable = true)
  ))

  val products: StructType = StructType(Seq(
    StructField("product_id", StringType, nullable = false),
    StructField("product_name", StringType, nullable = true),
    StructField("category", StringType, nullable = true),
    StructField("brand", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("cost", DoubleType, nullable = true)
  ))

  val orders: StructType = StructType(Seq(
    StructField("order_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("order_time", StringType, nullable = true),
    StructField("pay_time", StringType, nullable = true),
    StructField("province", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("order_status", StringType, nullable = true),
    StructField("total_amount", DoubleType, nullable = true),
    StructField("discount_amount", DoubleType, nullable = true),
    StructField("pay_amount", DoubleType, nullable = true)
  ))

  val orderItems: StructType = StructType(Seq(
    StructField("item_id", StringType, nullable = false),
    StructField("order_id", StringType, nullable = false),
    StructField("product_id", StringType, nullable = false),
    StructField("category", StringType, nullable = true),
    StructField("quantity", IntegerType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("amount", DoubleType, nullable = true)
  ))

  val orderEvent: StructType = StructType(Seq(
    StructField("order_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("order_time", StringType, nullable = true),
    StructField("province", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("category", StringType, nullable = true),
    StructField("pay_amount", DoubleType, nullable = true),
    StructField("order_status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true)
  ))
}

final case class RawTables(
  users: DataFrame,
  products: DataFrame,
  orders: DataFrame,
  orderItems: DataFrame
) {
  def validate(): Unit = {
    DataValidation.requireNonEmpty(users, "users.csv")
    DataValidation.requireNonEmpty(products, "products.csv")
    DataValidation.requireNonEmpty(orders, "orders.csv")
    DataValidation.requireNonEmpty(orderItems, "order_items.csv")
  }
}

object EcommerceData {
  def readAll(spark: SparkSession, inputRoot: String): RawTables = {
    val users = readCsv(spark, file(inputRoot, "users.csv"), Schemas.users)
      .withColumn("age", col("age").cast(IntegerType))
      .withColumn("register_date", to_date(col("register_date"), "yyyy-MM-dd"))

    val products = readCsv(spark, file(inputRoot, "products.csv"), Schemas.products)
      .withColumn("price", coalesce(col("price").cast(DoubleType), lit(0.0)))
      .withColumn("cost", coalesce(col("cost").cast(DoubleType), lit(0.0)))

    val orders = readCsv(spark, file(inputRoot, "orders.csv"), Schemas.orders)
      .withColumn("order_ts", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("pay_ts", to_timestamp(col("pay_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("stat_date", to_date(col("order_ts")))
      .withColumn("stat_hour", hour(col("order_ts")))
      .withColumn("total_amount", coalesce(col("total_amount").cast(DoubleType), lit(0.0)))
      .withColumn("discount_amount", coalesce(col("discount_amount").cast(DoubleType), lit(0.0)))
      .withColumn("pay_amount", coalesce(col("pay_amount").cast(DoubleType), lit(0.0)))
      .filter(col("order_id").isNotNull && col("user_id").isNotNull && col("order_ts").isNotNull)

    val orderItems = readCsv(spark, file(inputRoot, "order_items.csv"), Schemas.orderItems)
      .withColumn("quantity", coalesce(col("quantity").cast(IntegerType), lit(0)))
      .withColumn("price", coalesce(col("price").cast(DoubleType), lit(0.0)))
      .withColumn("amount", coalesce(col("amount").cast(DoubleType), lit(0.0)))
      .filter(col("item_id").isNotNull && col("order_id").isNotNull && col("product_id").isNotNull)

    RawTables(users, products, orders, orderItems)
  }

  private def readCsv(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("encoding", "UTF-8")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv(path)
  }

  private def file(inputRoot: String, fileName: String): String = {
    inputRoot.stripSuffix("/") + "/" + fileName
  }
}

object ProductCategory {
  def attach(orderItems: DataFrame, products: DataFrame): DataFrame = {
    val productDim = products
      .select(
        col("product_id"),
        col("category").as("product_category"),
        col("brand"),
        col("price").as("listed_price"),
        col("cost").as("listed_cost")
      )

    orderItems
      .join(productDim, Seq("product_id"), "left")
      .withColumn("final_category", coalesce(col("product_category"), col("category")))
      .filter(col("final_category").isNotNull)
  }
}

object MysqlSink {
  def writeFullRefresh(tableName: String, df: DataFrame, config: JobConfig): Unit = {
    val rowCount = df.count()
    println("Result table " + tableName + " rows: " + rowCount)

    if (config.dryRun) {
      df.show(20, truncate = false)
      return
    }

    df.coalesce(config.writePartitions)
      .write
      .format("jdbc")
      .option("url", config.jdbcUrl)
      .option("dbtable", tableName)
      .option("user", config.jdbcUser)
      .option("password", config.jdbcPassword)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("batchsize", "1000")
      .option("truncate", "true")
      .mode(SaveMode.Overwrite)
      .save()
  }
}

object DataValidation {
  def requireNonEmpty(df: DataFrame, name: String): Unit = {
    if (df.head(1).isEmpty) {
      throw new IllegalStateException(name + " is empty or cannot be read")
    }
  }
}