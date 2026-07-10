#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate synthetic Chinese ecommerce datasets for the Spark project.

Outputs:
  - users.csv
  - products.csv
  - orders.csv
  - order_items.csv
  - order_events.jsonl
  - dataset_schema.md

The generator is deliberately not pure random noise. It injects business
patterns that the Spark jobs can later defend in analysis and presentation:
evening peak, weekend lift, promotion-day lift, province consumption skew,
member-level purchasing power, and category-level sales differences.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


DEFAULT_SEED = 20260624
DEFAULT_USERS = 12_000
DEFAULT_PRODUCTS = 800
DEFAULT_ORDERS = 60_000
DEFAULT_START_DATE = "2025-01-01"
DEFAULT_END_DATE = "2026-06-23"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


PROVINCES: Dict[str, List[str]] = {
    "广东省": ["广州", "深圳", "佛山", "东莞", "珠海"],
    "江苏省": ["南京", "苏州", "无锡", "常州", "南通"],
    "浙江省": ["杭州", "宁波", "温州", "绍兴", "嘉兴"],
    "山东省": ["济南", "青岛", "烟台", "潍坊", "临沂"],
    "河南省": ["郑州", "洛阳", "南阳", "新乡", "许昌"],
    "四川省": ["成都", "绵阳", "德阳", "宜宾", "南充"],
    "湖北省": ["武汉", "襄阳", "宜昌", "荆州", "黄冈"],
    "湖南省": ["长沙", "株洲", "岳阳", "衡阳", "常德"],
    "福建省": ["福州", "厦门", "泉州", "漳州", "莆田"],
    "北京市": ["北京"],
    "上海市": ["上海"],
    "重庆市": ["重庆"],
    "陕西省": ["西安", "咸阳", "宝鸡", "渭南"],
    "河北省": ["石家庄", "唐山", "保定", "廊坊"],
    "安徽省": ["合肥", "芜湖", "阜阳", "安庆"],
}

PROVINCE_WEIGHTS: Dict[str, int] = {
    "广东省": 14,
    "江苏省": 11,
    "浙江省": 10,
    "山东省": 9,
    "河南省": 8,
    "四川省": 7,
    "湖北省": 6,
    "湖南省": 5,
    "福建省": 5,
    "北京市": 5,
    "上海市": 5,
    "重庆市": 4,
    "陕西省": 4,
    "河北省": 4,
    "安徽省": 3,
}

CATEGORIES: Dict[str, Dict[str, object]] = {
    "服饰": {
        "weight": 19,
        "price_range": (59, 899),
        "brands": ["森木", "青禾", "北岸", "简衣", "云上"],
        "nouns": ["卫衣", "衬衫", "连衣裙", "外套", "休闲裤", "运动鞋"],
    },
    "美妆": {
        "weight": 16,
        "price_range": (29, 699),
        "brands": ["花漾", "清颜", "鹿屿", "初白", "兰沁"],
        "nouns": ["面霜", "精华液", "口红", "洁面乳", "防晒霜", "粉底液"],
    },
    "数码": {
        "weight": 14,
        "price_range": (99, 6999),
        "brands": ["星联", "云启", "极客派", "晨芯", "智造者"],
        "nouns": ["蓝牙耳机", "机械键盘", "平板电脑", "智能手表", "显示器", "路由器"],
    },
    "食品": {
        "weight": 14,
        "price_range": (15, 399),
        "brands": ["谷小满", "鲜语", "山田记", "味来", "麦禾"],
        "nouns": ["坚果礼盒", "速溶咖啡", "牛肉干", "曲奇饼干", "酸奶", "茶叶"],
    },
    "家居": {
        "weight": 10,
        "price_range": (39, 1999),
        "brands": ["木里", "住造", "晴舍", "宅研", "柔光"],
        "nouns": ["台灯", "四件套", "收纳柜", "空气炸锅", "靠垫", "餐具套装"],
    },
    "母婴": {
        "weight": 8,
        "price_range": (25, 1299),
        "brands": ["贝童", "小树苗", "亲贝", "暖芽", "安心宝"],
        "nouns": ["纸尿裤", "奶瓶", "儿童湿巾", "婴儿推车", "积木", "儿童餐椅"],
    },
    "运动": {
        "weight": 7,
        "price_range": (39, 2599),
        "brands": ["跃动", "山野", "风速", "力场", "轻跑"],
        "nouns": ["瑜伽垫", "跑步机", "篮球", "运动背包", "骑行头盔", "哑铃"],
    },
    "图书": {
        "weight": 6,
        "price_range": (19, 299),
        "brands": ["新知", "灯塔", "墨舟", "青卷", "远读"],
        "nouns": ["管理书", "小说", "编程书", "绘本", "考试教材", "历史书"],
    },
    "家电": {
        "weight": 6,
        "price_range": (199, 5999),
        "brands": ["白鲸", "澄光", "云电", "恒净", "小方"],
        "nouns": ["洗衣机", "扫地机器人", "电饭煲", "净水器", "吹风机", "冰箱"],
    },
}

MEMBER_LEVELS: Dict[str, Dict[str, float]] = {
    "普通": {"weight": 55, "price_factor": 0.90, "discount": 0.00},
    "白银": {"weight": 23, "price_factor": 1.00, "discount": 0.01},
    "黄金": {"weight": 14, "price_factor": 1.15, "discount": 0.02},
    "铂金": {"weight": 6, "price_factor": 1.35, "discount": 0.03},
    "黑金": {"weight": 2, "price_factor": 1.70, "discount": 0.05},
}

CHANNELS = ["APP", "小程序", "PC", "直播间", "搜索广告", "自然流量"]
CHANNEL_WEIGHTS = [44, 22, 13, 9, 7, 5]

STATUSES = ["已支付", "已完成", "已取消", "已退款"]
STATUS_WEIGHTS = [70, 22, 5, 3]

PROMOTION_DATES = {
    "2025-01-01",
    "2025-02-14",
    "2025-03-08",
    "2025-05-01",
    "2025-06-18",
    "2025-10-01",
    "2025-11-11",
    "2025-12-12",
    "2026-01-01",
    "2026-02-14",
    "2026-03-08",
    "2026-05-01",
    "2026-06-18",
}


@dataclass(frozen=True)
class User:
    user_id: str
    user_name: str
    gender: str
    age: int
    province: str
    city: str
    member_level: str
    register_date: str


@dataclass(frozen=True)
class Product:
    product_id: str
    product_name: str
    category: str
    brand: str
    price: float
    cost: float
    category_weight: int


@dataclass(frozen=True)
class OrderForEvent:
    order_id: str
    user_id: str
    order_time: str
    province: str
    city: str
    category: str
    pay_amount: float
    order_status: str
    channel: str


def weighted_choice(items: Sequence[str], weights: Sequence[float]) -> str:
    return random.choices(items, weights=weights, k=1)[0]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def money(value: float) -> str:
    return f"{max(value, 0.0):.2f}"


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def random_date_between(start: date, end: date) -> date:
    delta_days = (end - start).days
    if delta_days < 0:
        raise ValueError("start date must be before or equal to end date")
    return start + timedelta(days=random.randint(0, delta_days))


def random_order_date(start: date, end: date) -> date:
    """Bias order dates toward weekends and promotion dates."""
    while True:
        d = random_date_between(start, end)
        score = 1.0
        if d.weekday() >= 5:
            score *= 1.35
        if d.isoformat() in PROMOTION_DATES:
            score *= 4.8
        if d.day in (15, 25):
            score *= 1.25
        if random.random() < min(score / 5.0, 1.0):
            return d


def random_order_time(order_date: date) -> datetime:
    """Bias order times toward 20:00-22:00 and lunch break."""
    bucket = random.choices(
        ["night_peak", "lunch", "workday", "late"],
        weights=[42, 18, 30, 10],
        k=1,
    )[0]
    if bucket == "night_peak":
        hour = random.randint(20, 22)
    elif bucket == "lunch":
        hour = random.randint(11, 13)
    elif bucket == "late":
        hour = random.choice([0, 1, 22, 23])
    else:
        hour = random.randint(8, 19)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.combine(order_date, time(hour, minute, second))


def choose_province() -> Tuple[str, str]:
    provinces = list(PROVINCE_WEIGHTS.keys())
    weights = [PROVINCE_WEIGHTS[p] for p in provinces]
    province = weighted_choice(provinces, weights)
    city = random.choice(PROVINCES[province])
    return province, city


def choose_member_level() -> str:
    levels = list(MEMBER_LEVELS.keys())
    weights = [MEMBER_LEVELS[level]["weight"] for level in levels]
    return weighted_choice(levels, weights)


def choose_category(preferred_factor: float = 1.0) -> str:
    categories = list(CATEGORIES.keys())
    weights = [float(CATEGORIES[c]["weight"]) for c in categories]
    if preferred_factor != 1.0:
        weights = [w * preferred_factor if c in ("服饰", "美妆", "数码", "食品") else w for c, w in zip(categories, weights)]
    return weighted_choice(categories, weights)


def generate_users(count: int, start_date: date, end_date: date) -> List[User]:
    users: List[User] = []
    surnames = ["王", "李", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴", "徐", "孙", "胡", "朱", "高"]
    given = ["子涵", "雨桐", "明轩", "梓萱", "浩然", "佳怡", "思源", "欣然", "俊杰", "诗涵", "一诺", "嘉诚"]
    genders = ["男", "女"]
    register_start = start_date - timedelta(days=730)
    for i in range(1, count + 1):
        province, city = choose_province()
        member_level = choose_member_level()
        age_base = random.gauss(31, 9)
        age = max(16, min(65, int(round(age_base))))
        users.append(
            User(
                user_id=f"U{i:06d}",
                user_name=f"{random.choice(surnames)}{random.choice(given)}{i}",
                gender=weighted_choice(genders, [49, 51]),
                age=age,
                province=province,
                city=city,
                member_level=member_level,
                register_date=random_date_between(register_start, end_date).isoformat(),
            )
        )
    return users


def generate_products(count: int) -> List[Product]:
    products: List[Product] = []
    categories = list(CATEGORIES.keys())
    category_weights = [int(CATEGORIES[c]["weight"]) for c in categories]
    category_sequence = categories[:]
    while len(category_sequence) < count:
        category_sequence.append(weighted_choice(categories, category_weights))

    for i in range(1, count + 1):
        category = category_sequence[i - 1]
        meta = CATEGORIES[category]
        brand = random.choice(meta["brands"])  # type: ignore[index]
        noun = random.choice(meta["nouns"])  # type: ignore[index]
        low, high = meta["price_range"]  # type: ignore[index]

        # Log-normal prices keep most goods affordable while preserving high-value tails.
        raw = random.lognormvariate(math.log((low + high) / 4), 0.65)
        price = min(max(raw, low), high)
        if category in ("数码", "家电"):
            price *= random.choice([0.9, 1.0, 1.0, 1.2, 1.5])
        price = round(price, 2)
        gross_margin = random.uniform(0.18, 0.48)
        cost = round(price * (1 - gross_margin), 2)
        products.append(
            Product(
                product_id=f"P{i:05d}",
                product_name=f"{brand}{noun}{random.randint(1, 6)}代",
                category=category,
                brand=brand,
                price=price,
                cost=cost,
                category_weight=int(meta["weight"]),  # type: ignore[arg-type]
            )
        )
    return products


def write_users(path: Path, users: Iterable[User]) -> int:
    rows = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "user_name", "gender", "age", "province", "city", "member_level", "register_date"])
        for u in users:
            writer.writerow([u.user_id, u.user_name, u.gender, u.age, u.province, u.city, u.member_level, u.register_date])
            rows += 1
    return rows


def write_products(path: Path, products: Iterable[Product]) -> int:
    rows = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "brand", "price", "cost"])
        for p in products:
            writer.writerow([p.product_id, p.product_name, p.category, p.brand, money(p.price), money(p.cost)])
            rows += 1
    return rows


def build_product_index(products: Sequence[Product]) -> Dict[str, List[Product]]:
    index: Dict[str, List[Product]] = {}
    for p in products:
        index.setdefault(p.category, []).append(p)
    return index


def choose_products_for_order(product_index: Dict[str, List[Product]], item_count: int, is_promo: bool) -> List[Product]:
    chosen: List[Product] = []
    base_category = choose_category(preferred_factor=1.15 if is_promo else 1.0)
    association_map = {
        "数码": ["图书", "家电"],
        "服饰": ["美妆", "运动"],
        "美妆": ["服饰", "食品"],
        "食品": ["家居", "母婴"],
        "家居": ["家电", "食品"],
        "母婴": ["食品", "家居"],
        "运动": ["服饰", "食品"],
        "图书": ["数码", "家居"],
        "家电": ["家居", "数码"],
    }
    categories = [base_category]
    for _ in range(item_count - 1):
        if random.random() < 0.55:
            categories.append(random.choice(association_map.get(base_category, list(product_index.keys()))))
        else:
            categories.append(choose_category())
    for c in categories:
        chosen.append(random.choice(product_index[c]))
    return chosen


def item_count_for_order(member_level: str, is_promo: bool) -> int:
    weights = [45, 31, 15, 6, 3]
    if member_level in ("铂金", "黑金"):
        weights = [30, 33, 22, 10, 5]
    if is_promo:
        weights = [27, 32, 23, 12, 6]
    return random.choices([1, 2, 3, 4, 5], weights=weights, k=1)[0]


def quantity_for_item(category: str, is_promo: bool) -> int:
    if category in ("食品", "母婴"):
        weights = [55, 27, 11, 5, 2]
    else:
        weights = [74, 18, 5, 2, 1]
    if is_promo:
        weights = [55, 25, 12, 5, 3]
    return random.choices([1, 2, 3, 4, 5], weights=weights, k=1)[0]


def discount_rate(member_level: str, order_date: date, channel: str, total_amount: float) -> float:
    rate = MEMBER_LEVELS[member_level]["discount"]
    if order_date.isoformat() in PROMOTION_DATES:
        rate += random.uniform(0.06, 0.18)
    elif order_date.weekday() >= 5:
        rate += random.uniform(0.01, 0.04)
    if channel == "直播间":
        rate += random.uniform(0.02, 0.06)
    if total_amount >= 1000:
        rate += 0.03
    elif total_amount >= 500:
        rate += 0.015
    return min(rate, 0.35)


def write_orders_and_items(
    output_dir: Path,
    users: Sequence[User],
    products: Sequence[Product],
    order_count: int,
    start_date: date,
    end_date: date,
) -> Tuple[int, int, int]:
    product_index = build_product_index(products)
    users_by_id = {u.user_id: u for u in users}
    user_ids = [u.user_id for u in users]
    user_weights = [
        MEMBER_LEVELS[u.member_level]["price_factor"] * PROVINCE_WEIGHTS[u.province]
        for u in users
    ]

    orders_path = output_dir / "orders.csv"
    items_path = output_dir / "order_items.csv"
    events_path = output_dir / "order_events.jsonl"

    order_rows = 0
    item_rows = 0
    event_rows = 0

    with orders_path.open("w", newline="", encoding="utf-8") as orders_file, \
        items_path.open("w", newline="", encoding="utf-8") as items_file, \
        events_path.open("w", encoding="utf-8") as events_file:

        orders_writer = csv.writer(orders_file)
        items_writer = csv.writer(items_file)
        orders_writer.writerow([
            "order_id",
            "user_id",
            "order_time",
            "pay_time",
            "province",
            "city",
            "channel",
            "order_status",
            "total_amount",
            "discount_amount",
            "pay_amount",
        ])
        items_writer.writerow(["item_id", "order_id", "product_id", "category", "quantity", "price", "amount"])

        for i in range(1, order_count + 1):
            user_id = weighted_choice(user_ids, user_weights)
            user = users_by_id[user_id]
            order_date = random_order_date(start_date, end_date)
            order_dt = random_order_time(order_date)
            is_promo = order_date.isoformat() in PROMOTION_DATES
            channel = weighted_choice(CHANNELS, CHANNEL_WEIGHTS)
            status = weighted_choice(STATUSES, STATUS_WEIGHTS)
            item_count = item_count_for_order(user.member_level, is_promo)
            selected_products = choose_products_for_order(product_index, item_count, is_promo)

            order_id = f"O{order_dt.strftime('%Y%m%d')}{i:08d}"
            total_amount = 0.0
            category_amounts: Dict[str, float] = {}

            for item_index, product in enumerate(selected_products, start=1):
                quantity = quantity_for_item(product.category, is_promo)
                member_factor = MEMBER_LEVELS[user.member_level]["price_factor"]
                price_factor = random.uniform(0.96, 1.04) * (1 + (member_factor - 1) * 0.05)
                item_price = round(product.price * price_factor, 2)
                amount = round(item_price * quantity, 2)
                total_amount += amount
                category_amounts[product.category] = category_amounts.get(product.category, 0.0) + amount
                item_id = f"I{i:08d}{item_index:02d}"
                items_writer.writerow([
                    item_id,
                    order_id,
                    product.product_id,
                    product.category,
                    quantity,
                    money(item_price),
                    money(amount),
                ])
                item_rows += 1

            rate = discount_rate(user.member_level, order_date, channel, total_amount)
            discount_amount = round(total_amount * rate, 2)
            pay_amount = round(total_amount - discount_amount, 2)
            if status == "已取消":
                pay_time = ""
                pay_amount = 0.0
            else:
                pay_delay_minutes = random.randint(1, 30)
                pay_time = (order_dt + timedelta(minutes=pay_delay_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                if status == "已退款":
                    pay_amount = round(pay_amount * random.uniform(0.0, 0.30), 2)

            orders_writer.writerow([
                order_id,
                user.user_id,
                order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                pay_time,
                user.province,
                user.city,
                channel,
                status,
                money(total_amount),
                money(discount_amount),
                money(pay_amount),
            ])
            order_rows += 1

            if status in ("已支付", "已完成"):
                top_category = max(category_amounts.items(), key=lambda kv: kv[1])[0]
                event = OrderForEvent(
                    order_id=order_id,
                    user_id=user.user_id,
                    order_time=order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    province=user.province,
                    city=user.city,
                    category=top_category,
                    pay_amount=round(pay_amount, 2),
                    order_status=status,
                    channel=channel,
                )
                events_file.write(json.dumps(event.__dict__, ensure_ascii=False) + "\n")
                event_rows += 1

    return order_rows, item_rows, event_rows


def write_schema(path: Path) -> None:
    content = """# Ecommerce Dataset Schema

Generated files use UTF-8 encoding and comma-separated CSV unless noted.

## users.csv

| field | meaning |
|---|---|
| user_id | User primary key |
| user_name | Synthetic Chinese nickname |
| gender | 男 / 女 |
| age | User age |
| province | User province |
| city | User city |
| member_level | 普通 / 白银 / 黄金 / 铂金 / 黑金 |
| register_date | Registration date |

## products.csv

| field | meaning |
|---|---|
| product_id | Product primary key |
| product_name | Synthetic product name |
| category | First-level category |
| brand | Synthetic brand |
| price | Listed product price |
| cost | Estimated product cost |

## orders.csv

| field | meaning |
|---|---|
| order_id | Order primary key |
| user_id | Linked user_id |
| order_time | Order creation timestamp |
| pay_time | Payment timestamp, empty for canceled orders |
| province | Shipping province |
| city | Shipping city |
| channel | Order source channel |
| order_status | 已支付 / 已完成 / 已取消 / 已退款 |
| total_amount | Sum of order item amount |
| discount_amount | Discount amount |
| pay_amount | Actual paid amount |

## order_items.csv

| field | meaning |
|---|---|
| item_id | Order item primary key |
| order_id | Linked order_id |
| product_id | Linked product_id |
| category | Product category snapshot |
| quantity | Purchased quantity |
| price | Item unit price |
| amount | quantity * price |

## order_events.jsonl

One JSON object per paid or completed order. This is the Kafka producer source.

Fields: order_id, user_id, order_time, province, city, category, pay_amount,
order_status, channel.
"""
    path.write_text(content, encoding="utf-8")


def validate_args(args: argparse.Namespace) -> None:
    if args.users < 100:
        raise ValueError("--users should be at least 100")
    if args.products < len(CATEGORIES):
        raise ValueError(f"--products should be at least {len(CATEGORIES)}")
    if args.orders < 1:
        raise ValueError("--orders must be positive")
    parse_date(args.start_date)
    parse_date(args.end_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Spark ecommerce project datasets.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    parser.add_argument("--users", type=int, default=DEFAULT_USERS, help="Number of users")
    parser.add_argument("--products", type=int, default=DEFAULT_PRODUCTS, help="Number of products")
    parser.add_argument("--orders", type=int, default=DEFAULT_ORDERS, help="Number of orders")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Order start date, yyyy-mm-dd")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Order end date, yyyy-mm-dd")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for reproducible output")
    args = parser.parse_args()
    validate_args(args)

    random.seed(args.seed)
    output_dir = Path(args.output).expanduser().resolve()
    ensure_output_dir(output_dir)

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    print(f"Output directory: {output_dir}")
    print(f"Seed: {args.seed}")
    print("Generating users...")
    users = generate_users(args.users, start_date, end_date)
    users_rows = write_users(output_dir / "users.csv", users)

    print("Generating products...")
    products = generate_products(args.products)
    products_rows = write_products(output_dir / "products.csv", products)

    print("Generating orders, order items, and realtime events...")
    orders_rows, item_rows, event_rows = write_orders_and_items(output_dir, users, products, args.orders, start_date, end_date)

    write_schema(output_dir / "dataset_schema.md")

    print("\nDone.")
    print(f"users.csv: {users_rows:,} rows")
    print(f"products.csv: {products_rows:,} rows")
    print(f"orders.csv: {orders_rows:,} rows")
    print(f"order_items.csv: {item_rows:,} rows")
    print(f"order_events.jsonl: {event_rows:,} events")
    print(f"dataset_schema.md: written")
    print("\nSuggested HDFS target:")
    print("hdfs dfs -mkdir -p /user/czm/ecommerce/raw")
    for file_name in ["users.csv", "products.csv", "orders.csv", "order_items.csv"]:
        print(f"hdfs dfs -put -f {output_dir / file_name} /user/czm/ecommerce/raw/")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise
