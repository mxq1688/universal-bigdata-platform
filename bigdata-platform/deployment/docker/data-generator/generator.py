"""
模拟电商数据生成器 - 向 Kafka 推送数据
"""
import json, random, time, os
from datetime import datetime

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9094')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
INTERVAL = float(os.getenv('INTERVAL', '1.0'))

USERS = [f"user_{i:05d}" for i in range(1, 10001)]
PRODUCTS = [{"id": f"prod_{i:04d}", "name": f"商品{i}", "category": random.choice(["电子","服装","食品","家居","运动"]), "price": round(random.uniform(10,5000),2)} for i in range(1,501)]
CITIES = ["北京","上海","广州","深圳","杭州","成都","武汉","南京","西安","重庆"]
ACTIONS = ["view","search","cart","order","pay","refund"]
DEVICES = ["iOS","Android","Web","MiniApp"]

def gen_behavior():
    p = random.choice(PRODUCTS)
    return {"event_type":"user_behavior","user_id":random.choice(USERS),"product_id":p["id"],"category":p["category"],"action":random.choices(ACTIONS,weights=[40,20,15,12,10,3])[0],"device":random.choice(DEVICES),"city":random.choice(CITIES),"timestamp":datetime.now().isoformat()}

def gen_order():
    items = random.sample(PRODUCTS, random.randint(1,5))
    total = sum(i["price"]*random.randint(1,3) for i in items)
    return {"event_type":"order","order_id":f"ord_{int(time.time()*1000)}_{random.randint(1000,9999)}","user_id":random.choice(USERS),"items":[{"product_id":i["id"],"price":i["price"],"qty":random.randint(1,3)} for i in items],"total":round(total,2),"status":random.choices(["created","paid","shipped","completed","refunded"],weights=[20,40,20,15,5])[0],"city":random.choice(CITIES),"timestamp":datetime.now().isoformat()}

if __name__ == "__main__":
    print("🚀 数据生成器启动")
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=lambda v: json.dumps(v,ensure_ascii=False).encode('utf-8'))
        print(f"✅ 已连接 Kafka: {KAFKA_BOOTSTRAP}")
        count = 0
        while True:
            for _ in range(BATCH_SIZE):
                producer.send('user_behavior', value=gen_behavior())
                if random.random() < 0.2: producer.send('orders', value=gen_order())
                count += 1
            producer.flush()
            if count % 100 == 0: print(f"📤 已发送 {count} 条 | {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(INTERVAL)
    except Exception as e:
        print(f"❌ {e}")
        print("回退到文件模式，写入 /tmp/bigdata-generator/")
        os.makedirs("/tmp/bigdata-generator", exist_ok=True)
        count = 0
        while True:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(f"/tmp/bigdata-generator/data_{ts}.json","w") as f:
                for _ in range(BATCH_SIZE): f.write(json.dumps(gen_behavior(),ensure_ascii=False)+"\n")
            count += BATCH_SIZE
            if count % 100 == 0: print(f"📁 已生成 {count} 条")
            time.sleep(INTERVAL)
