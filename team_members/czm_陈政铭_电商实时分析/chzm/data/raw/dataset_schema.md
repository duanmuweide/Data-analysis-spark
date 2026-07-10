# Ecommerce Dataset Schema

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
