# ecommerce_analysis 运行说明

## 版本适配

当前按你提供的虚拟机环境适配：

```text
Hadoop 3.3.6
Spark 2.4.6
Kafka 2.5.0
MySQL
JDK 13
```

`pom.xml` 目前使用：

```text
Scala 2.11.12
Spark 2.4.6
spark-sql-kafka-0-10_2.11 2.4.6
mysql-connector-java 8.0.33
```

严肃提醒：Spark 2.4.6 常见发行包默认是 Scala 2.11，所以这里先用 `_2.11`。但你必须在 master 虚拟机执行：

```bash
spark-submit --version
```

如果输出里写的是 `Scala version 2.12.x`，就把 `pom.xml` 改成：

```xml
<scala.version>2.12.10</scala.version>
<scala.binary.version>2.12</scala.binary.version>
```

Kafka 服务端是 `kafka_2.12-2.5.0` 不等于 Spark 应用必须用 Scala 2.12。Spark 应用依赖必须跟 Spark 自己的 Scala 版本一致。

## 代码入口

离线分析入口：

```text
com.niit.spark.ecommerce.OfflineJob
```

实时分析入口：

```text
com.niit.spark.ecommerce.StreamingJob
```

离线 4 个分析：

```text
A1 ads_sales_time_trend              时间序列统计
A2 ads_category_sales_rank           多维聚合 + TopN 排名
A3 ads_user_value_level              RFM 用户价值分层
A4 ads_category_association_rules    品类关联规则
```

实时 1 个分析：

```text
RT rt_category_window_sales          Kafka 订单流滑动窗口品类销售统计
```

## 1. 初始化 MySQL

在 master 虚拟机执行：

```bash
cd /opt/czm/ecommerce_analysis
mysql -u root -p < scripts/mysql_init.sql
```

这会创建数据库、结果表和 `spark` 用户：

```text
spark_ecommerce
spark / spark123456
```

如果 MySQL 没有开放远程访问，YARN 上的 executor 可能写不进去。至少要保证 slave 节点能访问：

```bash
mysql -h master-pc -u spark -p spark_ecommerce
```

## 2. 上传离线 CSV 到 HDFS

把这 4 个文件放到 master 后执行：

```bash
hdfs dfs -mkdir -p /user/czm/ecommerce/raw
hdfs dfs -put -f users.csv /user/czm/ecommerce/raw/
hdfs dfs -put -f products.csv /user/czm/ecommerce/raw/
hdfs dfs -put -f orders.csv /user/czm/ecommerce/raw/
hdfs dfs -put -f order_items.csv /user/czm/ecommerce/raw/
hdfs dfs -ls /user/czm/ecommerce/raw
```

`order_events.jsonl` 是实时 Kafka producer 用的，不属于离线分析输入。

## 3. 在 IDEA 里打胖包

用 IDEA 右侧 Maven 面板执行：

```text
Lifecycle -> clean
Lifecycle -> package
```

生成目标一般是：

```text
target/ecommerce-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar
```

如果你本机 JDK 下载 Maven 依赖报证书错误，可以在命令行使用项目内镜像配置：

```bash
mvn -s maven-settings.xml clean package -DskipTests
```

## 4. 上传 jar 到 master

建议目录：

```bash
/opt/czm/ecommerce_analysis
```

至少包含：

```text
ecommerce-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar
scripts/mysql_init.sql
```

## 5. 先 dry-run 离线任务

不写 MySQL，只打印结果样例：

```bash
spark-submit \
  --class com.niit.spark.ecommerce.OfflineJob \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=6 \
  ecommerce-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --input /user/czm/ecommerce/raw \
  --dry-run
```

## 6. 正式执行离线任务

```bash
spark-submit \
  --class com.niit.spark.ecommerce.OfflineJob \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=6 \
  ecommerce-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --input /user/czm/ecommerce/raw \
  --jdbc-url "jdbc:mysql://master-pc:3306/spark_ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true" \
  --jdbc-user spark \
  --jdbc-password spark123456
```

## 7. 检查离线结果

```sql
USE spark_ecommerce;
SELECT COUNT(*) FROM ads_sales_time_trend;
SELECT COUNT(*) FROM ads_category_sales_rank;
SELECT COUNT(*) FROM ads_user_value_level;
SELECT COUNT(*) FROM ads_category_association_rules;

SELECT * FROM ads_sales_time_trend ORDER BY stat_date, stat_hour LIMIT 10;
SELECT * FROM ads_category_sales_rank ORDER BY stat_date, category_rank LIMIT 10;
SELECT * FROM ads_user_value_level ORDER BY total_score DESC LIMIT 10;
SELECT * FROM ads_category_association_rules ORDER BY lift DESC LIMIT 10;
```

## 8. 实时链路执行顺序

实时不要先跑 producer。顺序必须是：

```text
启动 Kafka
创建 topic
启动 StreamingJob
再运行 kafka_order_producer.py
检查 MySQL 实时表
```

创建 topic：

```bash
kafka-topics.sh --create \
  --bootstrap-server master-pc:9092 \
  --topic czm_order_events \
  --partitions 1 \
  --replication-factor 1
```

启动实时 Spark 任务：

```bash
spark-submit \
  --class com.niit.spark.ecommerce.StreamingJob \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=6 \
  ecommerce-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --kafka-bootstrap master-pc:9092 \
  --topic czm_order_events \
  --checkpoint /user/czm/ecommerce/checkpoint/rt_category_window_sales \
  --jdbc-url "jdbc:mysql://master-pc:3306/spark_ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true" \
  --jdbc-user spark \
  --jdbc-password spark123456
```

另开一个窗口运行 producer：

```bash
python3 kafka_order_producer.py \
  --input order_events.jsonl \
  --bootstrap-server master-pc:9092 \
  --topic czm_order_events \
  --speed 10
```

检查实时结果：

```sql
USE spark_ecommerce;
SELECT * FROM rt_category_window_sales ORDER BY update_time DESC LIMIT 20;
```