#!/bin/bash
# ============================================================
# Kafka 数据模拟器 (纯 Shell) — Steam 游戏实时上架事件
#
# 零依赖，只需 Kafka 自带的 kafka-console-producer
#
# 用法:
#   chmod +x kafka_producer.sh
#   ./kafka_producer.sh                          # 默认连本地伪集群
#   ./kafka_producer.sh localhost:9092           # 指定 bootstrap
#   ./kafka_producer.sh localhost:9091 10        # 每秒 10 条
#   CTRL+C 停止
# ============================================================

# 自动查找 Kafka 安装目录
KAFKA_HOME=""
for d in /home/master/kafka_* /opt/kafka /usr/local/kafka ~/kafka; do
  [ -f "$d/bin/kafka-console-producer.sh" ] && { KAFKA_HOME="$d"; break; }
done
if [ -z "$KAFKA_HOME" ]; then
  echo "[Producer] 找不到 Kafka，请设置 KAFKA_HOME 环境变量" >&2
  exit 1
fi

BOOTSTRAP="${1:-localhost:9091,localhost:9092,localhost:9093}"
RATE="${2:-3}"
TOPIC="steam-game-events"

PREFIXES=(Dragon Cyber Star Dark Pixel Mega Super Ultra
          Shadow Crystal Phantom Neon Turbo Hyper Omega Royal)
SUFFIXES=(Quest Wars Legends Chronicles Adventure Survival
          Racing Simulator Tactics Odyssey Rebirth Origins Fury)
GENRES=(Action Adventure RPG Strategy Simulation
        Casual Indie Sports Racing FPS Puzzle
        Multiplayer Single-player "Open World" Horror)
TAGS=(Atmospheric Difficult "Story Rich" Funny "Pixel Graphics"
      "Fast-Paced" Relaxing Competitive Co-op PvP
      2D 3D Realistic Stylized Retro)
PRICES=(0 0 2.99 4.99 9.99 14.99 19.99 29.99 49.99 59.99)
OWNERS=(0 1000 5000 20000 50000 100000 200000 500000)

rand_elem() {
  local arr=("$@")
  echo "${arr[$((RANDOM % ${#arr[@]}))]}"
}

SLEEP_TIME=$(awk "BEGIN { printf \"%.4f\", 1.0 / $RATE }")

echo "[Producer] Bootstrap : $BOOTSTRAP"   >&2
echo "[Producer] Topic     : $TOPIC"        >&2
echo "[Producer] 速率      : ${RATE} 条/秒" >&2
echo "[Producer] 开始发送..."               >&2

COUNT=0
while true; do
  APP_ID=$((100000 + RANDOM % 900000))
  NAME="$(rand_elem "${PREFIXES[@]}") $(rand_elem "${SUFFIXES[@]}")"

  # 随机选 1-4 个类型
  N=$((1 + RANDOM % 4))
  GENRES_STR=""
  for ((i=0; i<N; i++)); do
    [ $i -gt 0 ] && GENRES_STR+=", "
    GENRES_STR+=$(rand_elem "${GENRES[@]}")
  done

  PRICE=$(rand_elem "${PRICES[@]}")
  OWNERS=$(rand_elem "${OWNERS[@]}")
  POS=$((RANDOM % 5001))
  NEG=$((RANDOM % 501))
  TS=$(date -Iseconds)

  # 输出 JSON 到 stdout → kafka-console-producer
  echo "{\"app_id\":$APP_ID,\"name\":\"$NAME\",\"price\":$PRICE,\"genres\":\"[$GENRES_STR]\",\"estimated_owners\":$OWNERS,\"positive\":$POS,\"negative\":$NEG,\"timestamp\":\"$TS\"}"

  COUNT=$((COUNT + 1))
  if [ $((COUNT % 10)) -eq 0 ]; then
    echo "[Producer] 已发送 $COUNT 条 | 最新: $NAME (\$$PRICE)" >&2
  fi

  sleep "$SLEEP_TIME"
done | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --broker-list "$BOOTSTRAP" \
  --topic "$TOPIC"

# kafka-console-producer 结束后才会到这里
echo "[Producer] 已停止。总计: $COUNT 条" >&2
