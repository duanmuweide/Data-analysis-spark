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
  // Spark Excel 读取器 (Steam数据为 .xlsx 格式)
  "com.crealytics" %% "spark-excel" % "3.5.0_0.20.4",
  // Web 服务框架 (用于结果展示的Web应用)
  "com.typesafe.akka" %% "akka-http" % "10.2.10",
  "com.typesafe.akka" %% "akka-actor" % "2.6.20",
  "com.typesafe.akka" %% "akka-stream" % "2.6.20",
  // JSON 处理
  "org.json4s" %% "json4s-jackson" % "4.0.6",
  // 模板引擎 (用于动态页面渲染)
  "org.thymeleaf" % "thymeleaf" % "3.1.2.RELEASE",
  // 测试
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "org.scalacheck" %% "scalacheck" % "1.17.0" % "test"
)

// 打包时排除 provided 依赖 (Spark集群已有)
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

// 主类入口
Compile / mainClass := Some("com.spark.project.Main")
