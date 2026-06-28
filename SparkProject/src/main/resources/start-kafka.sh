
#!/bin/bash
# ============================================================
# Kafka 实时分析启动脚本
# 用法: bash start-kafka.sh
# ============================================================

JAR_PATH="/home/master/sparkproject/SparkProject-1.0-SNAPSHOT.jar"
LOG_DIR="/home/master/sparkproject/logs"

# 创建日志目录
mkdir -p $LOG_DIR


echo "=========================================="
echo "  Kafka 实时分析 启动"
echo "  Jar:  $JAR_PATH"
echo "=========================================="
echo ""

# 1. 先启动 Consumer（后台运行，监听 Kafka 等待数据）
echo "[1/2] 启动 Consumer ..."
nohup java -cp $JAR_PATH com.qdu.kafka.Consumer \
    > $LOG_DIR/consumer.log 2>&1 &
CONSUMER_PID=$!
echo "       Consumer 已启动 (PID: $CONSUMER_PID)"
echo "       $CONSUMER_PID" > $LOG_DIR/consumer.pid
sleep 2  # 等 2 秒让 Consumer 完成初始化

# 2. 再启动 Producer（后台运行，开始推送数据）
echo "[2/2] 启动 Producer ..."
nohup java -cp $JAR_PATH com.qdu.kafka.Producer \
    > $LOG_DIR/producer.log 2>&1 &
PRODUCER_PID=$!
echo "       Producer 已启动 (PID: $PRODUCER_PID)"
echo "       $PRODUCER_PID" > $LOG_DIR/producer.pid

echo ""
echo "=========================================="
echo "  Kafka 实时分析 启动完毕"
echo "  Consumer PID: $CONSUMER_PID"
echo "  Producer PID: $PRODUCER_PID"
echo "  日志目录: $LOG_DIR"
echo "  停止脚本: bash stop-kafka.sh"
echo "=========================================="