#!/bin/bash
# ============================================================
# 修改 certain_analysis 表数据，演示实时变化
# 用法: bash update-certain.sh
# ============================================================

JAR_PATH="/home/master/sparkproject/SparkProject-1.0-SNAPSHOT.jar"

java -cp $JAR_PATH com.qdu.kafka.UpdateCertain
