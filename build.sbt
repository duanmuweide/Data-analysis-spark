name := "Sem6SparkProject"

version := "1.0.0"

scalaVersion := "2.12.18"

// Spark 版本
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  // Spark SQL (DataFrame / DataSet / SparkSQL)
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // Spark Streaming (实时流处理)
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  // Spark MLlib (机器学习 - FP-Growth 关联规则 / K-Means聚类等)
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  // JSON 处理
  "org.json4s" %% "json4s-jackson" % "4.0.6",
  // MySQL Connector (分析结果持久化)
  "mysql" % "mysql-connector-java" % "8.0.33",
  // Spark SQL Kafka (实时流处理)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  // 测试
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "org.scalacheck" %% "scalacheck" % "1.17.0" % "test"
)

// 打包配置
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// 主类入口
Compile / mainClass := Some("com.spark.project.Main")
