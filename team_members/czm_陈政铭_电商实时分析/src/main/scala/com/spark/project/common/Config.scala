package com.spark.project.common

/**
 * 全局配置常量
 *
 * Steam 游戏数据分析项目配置：
 * - 数据路径
 * - Web 服务端口
 * - Spark 参数
 * - 实时流配置
 *
 * TODO: 根据实际集群环境修改路径和参数
 */
object Config {

  // ==================== Web 服务配置 ====================

  /** Web 服务器监听端口 */
  val WEB_PORT: Int = 8080

  /** Web 服务器绑定地址 */
  val WEB_HOST: String = "0.0.0.0"

  // ==================== 数据路径配置 ====================

  /** 数据根目录（HDFS 路径，集群提交时使用） */
  val DATA_ROOT: String = "/data/member1"

  /** Steam 游戏主数据文件（Excel格式，390MB，115290 行） */
  val STEAM_DATA_FILE: String = s"$DATA_ROOT/games.csv"

  /** Steam 游戏样例数据（14行，开发调试用） */
  val STEAM_SAMPLE_FILE: String = s"$DATA_ROOT/games-example.csv"

  // ==================== Spark 配置 ====================

  /** 默认并行度 */
  val DEFAULT_PARTITIONS: Int = 200

  /** Shuffle 分区数 */
  val SHUFFLE_PARTITIONS: Int = 200

  /** 自适应查询执行 */
  val ADAPTIVE_QUERY_ENABLED: Boolean = true

  // ==================== 实时流配置 ====================

  /** Streaming 批处理间隔（秒） */
  val STREAMING_BATCH_INTERVAL: Int = 10

  /** 模拟数据源端口 */
  val STREAMING_SOURCE_PORT: Int = 9999

  // ==================== 数据库配置（分析结果存储） ====================

  /** JDBC URL */
  val JDBC_URL: String = "jdbc:mysql://localhost:3306/spark_steam_games"

  /** 数据库用户名 */
  val DB_USER: String = "root"

  /** 数据库密码 */
  val DB_PASSWORD: String = "password"

  // ==================== 分析阈值 ====================

  /** 热门游戏最低推荐数 */
  val MIN_RECOMMENDATIONS: Int = 5000

  /** 高分游戏最低正面评价数 */
  val MIN_POSITIVE_REVIEWS: Int = 100

  /** 关联规则最小支持度 */
  val MIN_SUPPORT: Double = 0.01

  /** 关联规则最小置信度 */
  val MIN_CONFIDENCE: Double = 0.3
}
