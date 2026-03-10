#!/usr/bin/env python3
"""
Mock 数据生成器
用于本地开发和演示，可生成 MySQL 测试数据 / Kafka 模拟消息

用法:
  python generate_mock_data.py mysql --count 10000   # 写入 MySQL
  python generate_mock_data.py kafka --count 5000    # 发送 Kafka
  python generate_mock_data.py csv   --count 1000    # 输出 CSV 文件
"""
import os
import sys
import json
import random
import argparse
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MockDataGen')

# ====================== 常量 ======================
CITIES = [
    ("北京", "北京"), ("上海", "上海"), ("广州", "广东"), ("深圳", "广东"),
    ("杭州", "浙江"), ("成都", "四川"), ("武汉", "湖北"), ("南京", "江苏"),
    ("重庆", "重庆"), ("西安", "陕西"), ("长沙", "湖南"), ("苏州", "江苏"),
    ("天津", "天津"), ("郑州", "河南"), ("青岛", "山东"), ("大连", "辽宁"),
]
CATEGORIES = ["数码", "服装", "食品", "家居", "美妆", "运动", "图书", "母婴"]
BRANDS = ["品牌A", "品牌B", "品牌C", "品牌D", "品牌E", "品牌F"]
PAYMENT_METHODS = ["alipay", "wechat", "card", "cash"]
PLATFORMS = ["app", "web", "mini_program"]
STATUS_LIST = ["created", "paid", "shipped", "completed", "refunded"]
STATUS_WEIGHTS = [5, 25, 15, 45, 10]
ACTIONS = ["view", "click", "cart", "order", "pay", "search", "favorite"]
ACTION_WEIGHTS = [40, 25, 10, 8, 5, 10, 2]
DEVICES = ["iPhone", "Android", "iPad", "PC", "Mac"]
STORE_TYPES = ["flagship", "standard", "mini"]


def random_date(start_days_ago=30, end_days_ago=0):
    days = random.randint(end_days_ago, start_days_ago)
    dt = datetime.now() - timedelta(days=days, hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59))
    return dt


# ====================== 生成器 ======================

def generate_users(count=1000):
    users = []
    for i in range(1, count + 1):
        city, province = random.choice(CITIES)
        users.append({
            'user_id': f'user_{i:05d}',
            'username': f'用户{i}',
            'gender': random.choice([0, 1, 2]),
            'age': random.randint(18, 65),
            'city': city,
            'province': province,
            'register_time': random_date(365).strftime('%Y-%m-%d %H:%M:%S'),
            'vip_level': random.choices([0, 1, 2, 3, 4, 5], weights=[40, 25, 15, 10, 7, 3])[0],
            'phone': f'1{random.randint(3,9)}{random.randint(100000000, 999999999)}',
        })
    return users


def generate_products(count=200):
    products = []
    for i in range(1, count + 1):
        cat = random.choice(CATEGORIES)
        price = round(random.uniform(5, 5000), 2)
        products.append({
            'product_id': f'prod_{i:04d}',
            'product_name': f'{cat}商品{i}',
            'category': cat,
            'sub_category': f'{cat}_子类{random.randint(1, 5)}',
            'brand': random.choice(BRANDS),
            'price': price,
            'cost': round(price * random.uniform(0.3, 0.7), 2),
            'status': 'active',
        })
    return products


def generate_stores(count=50):
    stores = []
    for i in range(1, count + 1):
        city, province = random.choice(CITIES)
        stores.append({
            'store_id': f'store_{i:03d}',
            'store_name': f'{city}{random.choice(STORE_TYPES)}店{i}',
            'city': city,
            'province': province,
            'store_type': random.choice(STORE_TYPES),
            'open_date': random_date(730).strftime('%Y-%m-%d'),
            'status': 'open',
        })
    return stores


def generate_orders(count=10000, user_count=1000, product_count=200, store_count=50):
    orders = []
    for i in range(1, count + 1):
        city, province = random.choice(CITIES)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 2000), 2)
        total = round(unit_price * quantity, 2)
        discount = round(total * random.uniform(0, 0.3), 2)
        pay_amount = round(total - discount, 2)
        status = random.choices(STATUS_LIST, weights=STATUS_WEIGHTS)[0]
        create_time = random_date(30)

        orders.append({
            'order_id': f'ord_{i:06d}',
            'user_id': f'user_{random.randint(1, user_count):05d}',
            'store_id': f'store_{random.randint(1, store_count):03d}',
            'product_id': f'prod_{random.randint(1, product_count):04d}',
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total,
            'discount_amount': discount,
            'pay_amount': pay_amount,
            'status': status,
            'payment_method': random.choice(PAYMENT_METHODS),
            'city': city,
            'platform': random.choice(PLATFORMS),
            'create_time': create_time.strftime('%Y-%m-%d %H:%M:%S'),
            'pay_time': (create_time + timedelta(minutes=random.randint(1, 30))).strftime(
                '%Y-%m-%d %H:%M:%S') if status != 'created' else None,
        })
    return orders


def generate_user_behaviors(count=50000, user_count=1000, product_count=200):
    behaviors = []
    for i in range(count):
        action = random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]
        city, _ = random.choice(CITIES)
        event_time = random_date(7)
        behaviors.append({
            'user_id': f'user_{random.randint(1, user_count):05d}',
            'action': action,
            'product_id': f'prod_{random.randint(1, product_count):04d}',
            'category': random.choice(CATEGORIES),
            'timestamp': int(event_time.timestamp() * 1000),
            'device': random.choice(DEVICES),
            'city': city,
            'session_id': f'sess_{random.randint(1, count // 5):08d}',
        })
    return behaviors


# ====================== 输出 ======================

def write_to_mysql(users, products, stores, orders):
    """写入 MySQL"""
    try:
        import pymysql
    except ImportError:
        logger.error("pymysql 未安装: pip install pymysql")
        return False

    conn = pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'root123'),
        database=os.getenv('MYSQL_DB', 'bigdata_source'),
        charset='utf8mb4',
    )

    try:
        with conn.cursor() as cursor:
            # 用户
            for u in users:
                cursor.execute(
                    "INSERT IGNORE INTO users (user_id,username,gender,age,city,province,register_time,vip_level,phone) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (u['user_id'], u['username'], u['gender'], u['age'], u['city'],
                     u['province'], u['register_time'], u['vip_level'], u['phone'])
                )
            logger.info("✅ 写入 %s 条用户", len(users))

            # 商品
            for p in products:
                cursor.execute(
                    "INSERT IGNORE INTO products (product_id,product_name,category,sub_category,brand,price,cost,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (p['product_id'], p['product_name'], p['category'], p['sub_category'],
                     p['brand'], p['price'], p['cost'], p['status'])
                )
            logger.info("✅ 写入 %s 条商品", len(products))

            # 门店
            for s in stores:
                cursor.execute(
                    "INSERT IGNORE INTO stores (store_id,store_name,city,province,store_type,open_date,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (s['store_id'], s['store_name'], s['city'], s['province'],
                     s['store_type'], s['open_date'], s['status'])
                )
            logger.info("✅ 写入 %s 条门店", len(stores))

            # 订单
            for o in orders:
                cursor.execute(
                    "INSERT IGNORE INTO orders (order_id,user_id,store_id,product_id,quantity,"
                    "unit_price,total_amount,discount_amount,pay_amount,status,payment_method,"
                    "city,platform,create_time,pay_time) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (o['order_id'], o['user_id'], o['store_id'], o['product_id'], o['quantity'],
                     o['unit_price'], o['total_amount'], o['discount_amount'], o['pay_amount'],
                     o['status'], o['payment_method'], o['city'], o['platform'],
                     o['create_time'], o['pay_time'])
                )
            logger.info("✅ 写入 %s 条订单", len(orders))

        conn.commit()
        return True
    except Exception as e:
        logger.error("MySQL 写入失败: %s", e)
        conn.rollback()
        return False
    finally:
        conn.close()


def send_to_kafka(behaviors, orders):
    """发送到 Kafka"""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python 未安装: pip install kafka-python")
        return False

    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    )

    for b in behaviors:
        producer.send('user_behavior', value=b)
    logger.info("✅ 发送 %s 条行为到 Kafka topic: user_behavior", len(behaviors))

    for o in orders:
        producer.send('order_events', value=o)
    logger.info("✅ 发送 %s 条订单到 Kafka topic: order_events", len(orders))

    producer.flush()
    producer.close()
    return True


def write_to_csv(users, products, stores, orders, behaviors):
    """输出 CSV"""
    import csv

    output_dir = 'mock_data'
    os.makedirs(output_dir, exist_ok=True)

    datasets = [
        ('users.csv', users),
        ('products.csv', products),
        ('stores.csv', stores),
        ('orders.csv', orders),
        ('user_behaviors.csv', behaviors),
    ]

    for filename, data in datasets:
        if not data:
            continue
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        logger.info("✅ 写入 %s: %s 条", filepath, len(data))

    return True


# ====================== CLI ======================

def main():
    parser = argparse.ArgumentParser(description='Mock 数据生成器')
    parser.add_argument('target', choices=['mysql', 'kafka', 'csv', 'all'],
                        help='输出目标')
    parser.add_argument('--count', type=int, default=10000, help='订单数量 (默认 10000)')
    parser.add_argument('--users', type=int, default=1000, help='用户数量')
    parser.add_argument('--products', type=int, default=200, help='商品数量')
    parser.add_argument('--stores', type=int, default=50, help='门店数量')
    parser.add_argument('--behaviors', type=int, default=50000, help='行为日志数量')

    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("🎲 开始生成 Mock 数据...")
    logger.info("  订单: %s, 用户: %s, 商品: %s, 门店: %s, 行为: %s",
                args.count, args.users, args.products, args.stores, args.behaviors)
    logger.info("=" * 50)

    users = generate_users(args.users)
    products = generate_products(args.products)
    stores = generate_stores(args.stores)
    orders = generate_orders(args.count, args.users, args.products, args.stores)
    behaviors = generate_user_behaviors(args.behaviors, args.users, args.products)

    if args.target in ('mysql', 'all'):
        write_to_mysql(users, products, stores, orders)

    if args.target in ('kafka', 'all'):
        send_to_kafka(behaviors, orders)

    if args.target in ('csv', 'all'):
        write_to_csv(users, products, stores, orders, behaviors)

    logger.info("🎉 Mock 数据生成完成!")


if __name__ == '__main__':
    main()
