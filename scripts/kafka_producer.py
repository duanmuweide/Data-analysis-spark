#!/usr/bin/env python3
"""
Kafka 数据模拟器 — 模拟 Steam 游戏实时上架事件

向 Kafka Topic 发送随机的游戏上架 JSON 消息，
供 Spark Streaming 消费和实时统计。

使用方式:
    python kafka_producer.py --bootstrap-server niit-master:9091,niit-master:9092,niit-master:9093 \
                              --topic steam-game-events --rate 5

依赖: pip install kafka-python
"""

import json
import random
import time
import argparse
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    print("[ERROR] 请安装 kafka-python: pip install kafka-python")
    exit(1)

# 模拟数据池
GAME_PREFIXES = [
    "Dragon", "Cyber", "Star", "Dark", "Pixel", "Mega", "Super", "Ultra",
    "Shadow", "Crystal", "Phantom", "Neon", "Turbo", "Hyper", "Omega", "Royal"
]
GAME_SUFFIXES = [
    "Quest", "Wars", "Legends", "Chronicles", "Adventure", "Survival",
    "Racing", "Simulator", "Tactics", "Odyssey", "Rebirth", "Origins", "Fury"
]
GENRES_POOL = [
    "Action", "Adventure", "RPG", "Strategy", "Simulation",
    "Casual", "Indie", "Sports", "Racing", "FPS", "Puzzle",
    "Multiplayer", "Single-player", "Open World", "Horror"
]
TAGS_POOL = [
    "Atmospheric", "Difficult", "Story Rich", "Funny", "Pixel Graphics",
    "Fast-Paced", "Relaxing", "Competitive", "Co-op", "PvP",
    "2D", "3D", "Realistic", "Stylized", "Retro"
]


def random_game():
    """生成随机游戏数据"""
    name = f"{random.choice(GAME_PREFIXES)} {random.choice(GAME_SUFFIXES)}"
    genres = random.sample(GENRES_POOL, k=random.randint(1, 4))
    tags = random.sample(TAGS_POOL, k=random.randint(1, 5))

    return {
        "app_id": random.randint(100000, 999999),
        "name": name,
        "price": round(random.choice([0, 0, 2.99, 4.99, 9.99, 14.99, 19.99, 29.99, 49.99, 59.99]), 2),
        "genres": str(genres).replace("'", ""),
        "estimated_owners": random.choice([0, 1000, 5000, 20000, 50000, 100000, 200000, 500000]),
        "positive": random.randint(0, 5000),
        "negative": random.randint(0, 500),
        "timestamp": datetime.now().isoformat()
    }


def main():
    parser = argparse.ArgumentParser(description="Steam 游戏实时数据模拟器")
    parser.add_argument("--bootstrap-server", default="localhost:9091,localhost:9092,localhost:9093",
                        help="Kafka Bootstrap Server 地址 (默认: localhost:9091,localhost:9092,localhost:9093)")
    parser.add_argument("--topic", default="steam-game-events",
                        help="Kafka Topic (默认: steam-game-events)")
    parser.add_argument("--rate", type=int, default=3,
                        help="每秒发送消息数 (默认: 3)")
    parser.add_argument("--duration", type=int, default=0,
                        help="运行时长(秒)，0=持续运行 (默认: 0)")
    args = parser.parse_args()

    # 创建 Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks=0,          # 不等待确认，追求吞吐
            linger_ms=5      # 批量发送
        )
        print(f"[Producer] 已连接 Kafka: {args.bootstrap_server}")
        print(f"[Producer] Topic: {args.topic}, 速率: {args.rate}/秒")
    except Exception as e:
        print(f"[Producer] Kafka 连接失败: {e}")
        print("[Producer] 请确保 Kafka 集群已启动")
        exit(1)

    count = 0
    start_time = time.time()

    try:
        while True:
            # 生成并发送消息
            game = random_game()
            producer.send(args.topic, value=game)
            count += 1

            if count % 10 == 0:
                print(f"[Producer] 已发送 {count} 条 | "
                      f"最新: {game['name']} (${game['price']})")

            # 控制发送速率
            time.sleep(1.0 / args.rate)

            # 检查运行时长
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break

    except KeyboardInterrupt:
        print(f"\n[Producer] 已停止。总计发送: {count} 条")
    finally:
        producer.flush()
        producer.close()
        print("[Producer] Kafka Producer 已关闭")


if __name__ == "__main__":
    main()
