-- =================================================================
-- Hive ODS 层 建表语句
-- =================================================================

-- ODS MySQL 订单快照
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_orders (
    order_id        STRING COMMENT '订单ID',
    user_id         STRING COMMENT '用户ID',
    store_id        STRING COMMENT '门店ID',
    product_id      STRING COMMENT '商品ID',
    quantity        INT COMMENT '数量',
    unit_price      DECIMAL(10,2) COMMENT '单价',
    total_amount    DECIMAL(10,2) COMMENT '总金额',
    discount_amount DECIMAL(10,2) COMMENT '优惠金额',
    pay_amount      DECIMAL(10,2) COMMENT '实付金额',
    status          STRING COMMENT '订单状态',
    payment_method  STRING COMMENT '支付方式',
    city            STRING COMMENT '下单城市',
    platform        STRING COMMENT '下单平台',
    create_time     STRING COMMENT '下单时间',
    pay_time        STRING COMMENT '支付时间',
    update_time     STRING COMMENT '更新时间'
)
PARTITIONED BY (dt STRING COMMENT '同步日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/ods.db/ods_orders'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS MySQL 用户快照
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_users (
    user_id         STRING COMMENT '用户ID',
    username        STRING COMMENT '用户名',
    gender          INT COMMENT '性别',
    age             INT COMMENT '年龄',
    city            STRING COMMENT '城市',
    province        STRING COMMENT '省份',
    register_time   STRING COMMENT '注册时间',
    vip_level       INT COMMENT 'VIP等级',
    phone           STRING COMMENT '手机号',
    create_time     STRING COMMENT '创建时间',
    update_time     STRING COMMENT '更新时间'
)
PARTITIONED BY (dt STRING COMMENT '同步日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/ods.db/ods_users'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS MySQL 商品快照
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_products (
    product_id      STRING COMMENT '商品ID',
    product_name    STRING COMMENT '商品名称',
    category        STRING COMMENT '一级分类',
    sub_category    STRING COMMENT '二级分类',
    brand           STRING COMMENT '品牌',
    price           DECIMAL(10,2) COMMENT '价格',
    cost            DECIMAL(10,2) COMMENT '成本',
    status          STRING COMMENT '状态',
    create_time     STRING COMMENT '创建时间',
    update_time     STRING COMMENT '更新时间'
)
PARTITIONED BY (dt STRING COMMENT '同步日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/user/hive/warehouse/ods.db/ods_products'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS Kafka 用户行为日志
CREATE EXTERNAL TABLE IF NOT EXISTS ods_kafka.ods_user_behavior (
    user_id         STRING COMMENT '用户ID',
    action          STRING COMMENT '行为类型',
    product_id      STRING COMMENT '商品ID',
    category        STRING COMMENT '商品分类',
    `timestamp`     BIGINT COMMENT '事件时间戳(ms)',
    device          STRING COMMENT '设备',
    city            STRING COMMENT '城市',
    session_id      STRING COMMENT '会话ID'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/ods_kafka.db/ods_user_behavior';

-- ODS Kafka 订单事件
CREATE EXTERNAL TABLE IF NOT EXISTS ods_kafka.ods_order_events (
    order_id        STRING COMMENT '订单ID',
    user_id         STRING COMMENT '用户ID',
    store_id        STRING COMMENT '门店ID',
    amount          DECIMAL(10,2) COMMENT '金额',
    status          STRING COMMENT '订单状态',
    payment_method  STRING COMMENT '支付方式',
    city            STRING COMMENT '城市',
    `timestamp`     BIGINT COMMENT '事件时间戳(ms)'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/ods_kafka.db/ods_order_events';
