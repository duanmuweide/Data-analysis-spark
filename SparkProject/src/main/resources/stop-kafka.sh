#!/bin/bash
# ============================================================
# Kafka 实时分析停止脚本
# 用法: bash stop-kafka.sh
# ============================================================

LOG_DIR="/home/master/sparkproject/logs"

echo "=========================================="
echo "  Kafka 实时分析 停止"
echo "=========================================="
echo ""

# 停止 Consumer
if [ -f $LOG_DIR/consumer.pid ]; then
    PID=$(cat $LOG_DIR/consumer.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "[1/2] 停止 Consumer (PID: $PID) ..."
        kill $PID
        echo "      Consumer 已停止"
    else
        echo "[1/2] Consumer (PID: $PID) 已不在运行"
    fi
    rm -f $LOG_DIR/consumer.pid
else
    echo "[1/2] 未找到 Consumer PID 文件"
fi

# 停止 Producer
if [ -f $LOG_DIR/producer.pid ]; then
    PID=$(cat $LOG_DIR/producer.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "[2/2] 停止 Producer (PID: $PID) ..."
        kill $PID
        echo "      Producer 已停止"
    else
        echo "[2/2] Producer (PID: $PID) 已不在运行"
    fi
    rm -f $LOG_DIR/producer.pid
else
    echo "[2/2] 未找到 Producer PID 文件"
fi

echo ""
echo "=========================================="
echo "  Kafka 实时分析 已停止"
echo "=========================================="
