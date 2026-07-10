from flask import Blueprint, jsonify, request

from db import json_ready, ping, query_all, query_one


analysis_bp = Blueprint("analysis", __name__)

RESULT_TABLES = [
    "ads_sales_time_trend",
    "ads_category_sales_rank",
    "ads_user_value_level",
    "ads_category_association_rules",
    "rt_category_window_sales",
]


def api_success(data=None, **extra):
    payload = {"success": True, "data": json_ready(data if data is not None else {})}
    payload.update(json_ready(extra))
    return jsonify(payload)


def positive_int_arg(name: str, default: int, max_value: int) -> int:
    raw = request.args.get(name, "")
    try:
        value = int(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return min(value, max_value)


@analysis_bp.get("/health")
def health():
    tables = []
    database_ok = ping()
    if database_ok:
        for table in RESULT_TABLES:
            row = query_one(f"SELECT COUNT(*) AS row_count FROM {table}")
            tables.append({"table": table, "row_count": row["row_count"] if row else 0})

    return api_success({
        "database_ok": database_ok,
        "tables": tables,
    })


@analysis_bp.get("/sales/trend")
def sales_trend():
    summary = query_one(
        """
        SELECT
          COALESCE(SUM(order_count), 0) AS order_count,
          COALESCE(SUM(user_count), 0) AS user_count,
          ROUND(COALESCE(SUM(total_amount), 0), 2) AS total_amount,
          ROUND(COALESCE(SUM(discount_amount), 0), 2) AS discount_amount,
          ROUND(COALESCE(SUM(pay_amount), 0), 2) AS pay_amount,
          ROUND(COALESCE(SUM(pay_amount) / NULLIF(SUM(order_count), 0), 0), 2) AS avg_order_amount,
          DATE_FORMAT(MIN(stat_date), '%%Y-%%m-%%d') AS start_date,
          DATE_FORMAT(MAX(stat_date), '%%Y-%%m-%%d') AS end_date,
          MAX(update_time) AS last_update
        FROM ads_sales_time_trend
        """
    ) or {}

    daily = query_all(
        """
        SELECT
          DATE_FORMAT(stat_date, '%%Y-%%m-%%d') AS stat_date,
          SUM(order_count) AS order_count,
          SUM(user_count) AS user_count,
          ROUND(SUM(total_amount), 2) AS total_amount,
          ROUND(SUM(discount_amount), 2) AS discount_amount,
          ROUND(SUM(pay_amount), 2) AS pay_amount,
          ROUND(SUM(pay_amount) / NULLIF(SUM(order_count), 0), 2) AS avg_order_amount
        FROM ads_sales_time_trend
        GROUP BY stat_date
        ORDER BY stat_date
        """
    )

    hourly = query_all(
        """
        SELECT
          stat_hour,
          LPAD(stat_hour, 2, '0') AS stat_hour_label,
          SUM(order_count) AS order_count,
          ROUND(SUM(pay_amount), 2) AS pay_amount
        FROM ads_sales_time_trend
        GROUP BY stat_hour
        ORDER BY stat_hour
        """
    )

    return api_success({
        "summary": summary,
        "daily": daily,
        "hourly": hourly,
    })


@analysis_bp.get("/category/top")
def category_top():
    limit = positive_int_arg("limit", 10, 30)
    items = query_all(
        """
        SELECT
          category,
          SUM(order_count) AS order_count,
          SUM(quantity_sum) AS quantity_sum,
          ROUND(SUM(amount_sum), 2) AS amount_sum,
          ROUND(SUM(amount_sum) / NULLIF(SUM(quantity_sum), 0), 2) AS avg_unit_amount,
          MAX(category_rank) AS max_daily_rank,
          MAX(update_time) AS last_update
        FROM ads_category_sales_rank
        GROUP BY category
        ORDER BY amount_sum DESC, quantity_sum DESC
        LIMIT %s
        """,
        (limit,),
    )

    return api_success({"limit": limit, "items": items})


@analysis_bp.get("/user/value-level")
def user_value_level():
    summary = query_one(
        """
        SELECT
          COUNT(*) AS user_count,
          ROUND(AVG(recency_days), 2) AS avg_recency_days,
          ROUND(AVG(frequency), 2) AS avg_frequency,
          ROUND(AVG(monetary), 2) AS avg_monetary,
          MAX(update_time) AS last_update
        FROM ads_user_value_level
        """
    ) or {}

    levels = query_all(
        """
        SELECT
          user_level,
          COUNT(*) AS user_count,
          ROUND(COUNT(*) / NULLIF((SELECT COUNT(*) FROM ads_user_value_level), 0), 4) AS user_ratio,
          ROUND(AVG(monetary), 2) AS avg_monetary,
          ROUND(AVG(frequency), 2) AS avg_frequency
        FROM ads_user_value_level
        GROUP BY user_level
        ORDER BY FIELD(user_level, '高价值用户', '重点发展用户', '一般保持用户', '低价值用户')
        """
    )

    member_levels = query_all(
        """
        SELECT
          COALESCE(member_level, '未知') AS member_level,
          user_level,
          COUNT(*) AS user_count
        FROM ads_user_value_level
        GROUP BY member_level, user_level
        ORDER BY FIELD(COALESCE(member_level, '未知'), '普通', '白银', '黄金', '铂金', '黑金', '未知'),
                 FIELD(user_level, '高价值用户', '重点发展用户', '一般保持用户', '低价值用户')
        """
    )

    return api_success({
        "summary": summary,
        "levels": levels,
        "member_levels": member_levels,
    })


@analysis_bp.get("/category/association")
def category_association():
    limit = positive_int_arg("limit", 10, 30)
    rules = query_all(
        """
        SELECT
          antecedent,
          consequent,
          support,
          confidence,
          lift,
          pair_order_count,
          antecedent_order_count,
          consequent_order_count,
          total_basket_count,
          update_time
        FROM ads_category_association_rules
        ORDER BY lift DESC, confidence DESC, support DESC
        LIMIT %s
        """,
        (limit,),
    )

    nodes_by_name = {}
    links = []
    for row in rules:
        source = row["antecedent"]
        target = row["consequent"]
        nodes_by_name[source] = {"name": source}
        nodes_by_name[target] = {"name": target}
        links.append({
            "source": source,
            "target": target,
            "value": float(row["lift"] or 0),
            "lift": float(row["lift"] or 0),
            "confidence": float(row["confidence"] or 0),
            "support": float(row["support"] or 0),
        })

    return api_success({
        "limit": limit,
        "rules": rules,
        "graph": {
            "nodes": list(nodes_by_name.values()),
            "links": links,
        },
    })


@analysis_bp.get("/realtime/category-window")
def realtime_category_window():
    limit = positive_int_arg("limit", 20, 100)
    latest_window = query_one(
        """
        SELECT
          MAX(window_end) AS latest_window_end,
          MAX(update_time) AS last_update,
          MAX(batch_id) AS latest_batch_id
        FROM rt_category_window_sales
        """
    ) or {}

    latest_items = []
    if latest_window.get("latest_window_end") is not None:
        latest_items = query_all(
            """
            SELECT
              window_start,
              window_end,
              category,
              order_count,
              pay_amount,
              batch_id,
              update_time
            FROM rt_category_window_sales
            WHERE window_end = %s
            ORDER BY pay_amount DESC, order_count DESC
            LIMIT %s
            """,
            (latest_window["latest_window_end"], limit),
        )

    recent = query_all(
        """
        SELECT
          window_start,
          window_end,
          category,
          order_count,
          pay_amount,
          batch_id,
          update_time
        FROM rt_category_window_sales
        ORDER BY update_time DESC, window_end DESC, pay_amount DESC
        LIMIT %s
        """,
        (limit,),
    )

    trend_desc = query_all(
        """
        SELECT
          window_end,
          SUM(order_count) AS order_count,
          ROUND(SUM(pay_amount), 2) AS pay_amount
        FROM rt_category_window_sales
        GROUP BY window_end
        ORDER BY window_end DESC
        LIMIT 20
        """
    )
    trend = list(reversed(trend_desc))

    return api_success({
        "summary": latest_window,
        "latest": latest_items,
        "recent": recent,
        "trend": trend,
    })
