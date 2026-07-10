#!/usr/bin/env python3
"""
模拟数据生成脚本

用于生成项目测试所需的样本数据（替代真实大数据集进行本地测试）。
真实数据集应满足: 200M-500M, 15000+ 条记录。

使用方式:
    python generate_sample_data.py --member 1 --size 1000
    python generate_sample_data.py --member 2 --size 1000
"""

import csv
import random
import argparse
from datetime import datetime, timedelta

# ============================================
# 成员1 - 电商数据生成器
# ============================================

def generate_ecommerce_data(num_records: int, output_path: str):
    """
    生成电商订单数据（成员1数据集A）

    字段:
    - order_id: 订单ID
    - user_id: 用户ID
    - product_id: 商品ID
    - category: 商品类别
    - amount: 订单金额
    - quantity: 购买数量
    - region_id: 区域ID
    - order_date: 下单时间
    """
    # TODO: 实现数据生成逻辑
    # 1. 生成 random user_id, product_id
    # 2. 生成合理范围内的 amount, quantity
    # 3. 生成时间范围内的 order_date
    # 4. 写入 CSV 文件
    pass


def generate_user_behavior_data(num_records: int, output_path: str):
    """
    生成用户行为数据（成员1数据集B）

    字段:
    - user_id: 用户ID
    - action_type: 行为类型(浏览/加购/收藏/购买)
    - product_id: 商品ID
    - action_time: 行为时间
    - session_id: 会话ID
    """
    # TODO: 实现数据生成逻辑
    pass


def generate_region_data(output_path: str):
    """
    生成区域维度数据（成员1数据集C）

    字段:
    - region_id: 区域ID
    - province: 省份
    - city: 城市
    - district: 区县
    """
    # TODO: 实现中国省份城市字典数据生成
    pass


# ============================================
# 成员2 - 社交媒体数据生成器
# ============================================

def generate_comment_data(num_records: int, output_path: str):
    """
    生成评论/反馈数据（成员2数据集A）

    字段:
    - comment_id: 评论ID
    - user_id: 用户ID
    - content: 评论内容
    - rating: 评分(1-5)
    - comment_time: 评论时间
    - product_id: 关联商品ID
    """
    # TODO: 实现评论数据生成（包括中文评论文本）
    pass


def generate_social_network_data(num_users: int, output_path: str):
    """
    生成社交关系数据（成员2数据集B）

    字段:
    - user_id: 用户ID
    - follow_id: 关注的用户ID
    - relation_type: 关系类型(follow/friend)
    """
    # TODO: 实现社交关系图数据生成
    pass


# ============================================
# 主入口
# ============================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spark项目模拟数据生成器')
    parser.add_argument('--member', type=int, required=True, choices=[1, 2],
                        help='成员编号 (1或2)')
    parser.add_argument('--size', type=int, default=1000,
                        help='生成记录数 (默认1000，用于本地测试)')
    parser.add_argument('--output', type=str, default='../data/',
                        help='输出目录')

    args = parser.parse_args()
    print(f"TODO: 为成员{args.member}生成 {args.size} 条模拟数据")
