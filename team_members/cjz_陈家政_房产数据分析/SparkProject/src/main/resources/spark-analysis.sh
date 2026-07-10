#!/bin/bash
# ============================================================
# Spark 离线分析批量执行脚本
# 用法: bash spark-analysis.sh
# ============================================================

set -e  # 任一命令失败则退出

MASTER_URL="spark://master-pc:7077"
JAR_PATH="/home/master/sparkproject/SparkProject-1.0-SNAPSHOT.jar"

echo "=========================================="
echo "  Spark 离线分析 开始执行"
echo "  Master: $MASTER_URL"
echo "  Jar:    $JAR_PATH"
echo "=========================================="
echo ""

# 1. 数据写入 MySQL
echo "[1/5] 执行 Write2Db ..."
spark-submit \
    --name writedata \
    --class com.qdu.jdbc.Write2Db \
    --master $MASTER_URL \
    $JAR_PATH
echo "[1/5] Write2Db 完成"
echo ""

# 2. 面积区间房价分析
echo "[2/5] 执行 AreaAnalysis ..."
spark-submit \
    --name area \
    --class com.qdu.jdbc.AreaAnalysis \
    --master $MASTER_URL \
    $JAR_PATH
echo "[2/5] AreaAnalysis 完成"
echo ""

# 3. 小区房价分析
echo "[3/5] 执行 CommunityAnalysis ..."
spark-submit \
    --name community \
    --class com.qdu.jdbc.CommunityAnalysis \
    --master $MASTER_URL \
    $JAR_PATH
echo "[3/5] CommunityAnalysis 完成"
echo ""

# 4. 市区房价分析
echo "[4/5] 执行 DistrictAnalysis ..."
spark-submit \
    --name district \
    --class com.qdu.jdbc.DistrictAnalysis \
    --master $MASTER_URL \
    $JAR_PATH
echo "[4/5] DistrictAnalysis 完成"
echo ""

# 5. 年份区间分析
echo "[5/5] 执行 YearAnalysis ..."
spark-submit \
    --name year \
    --class com.qdu.jdbc.YearAnalysis \
    --master $MASTER_URL \
    $JAR_PATH
echo "[5/5] YearAnalysis 完成"
echo ""

echo "=========================================="
echo "  全部 Spark 分析任务执行完毕"
echo "=========================================="
